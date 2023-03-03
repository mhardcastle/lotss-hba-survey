#!/usr/bin/python

# clone of monitor.py to do monitoring of full field reprocessing

from __future__ import print_function
from time import sleep
import datetime
from surveys_db import SurveysDB, tag_field, get_cluster

## need to update this
#from run_full_field_reprocessing_pipeline import stage_field
#from reprocessing_utils import prepare_field  ## ddf-pipeline utils
import os
import threading
import glob
import requests
import stager_access
from rclone import RClone   ## DO NOT pip3 install --user python-rclone -- use https://raw.githubusercontent.com/mhardcastle/ddf-pipeline/master/utils/rclone.py


#################################
## CLUSTER SPECIFICS - use environment variables

'''
export DDF_PIPELINE_CLUSTER=cosma
export LINC_DATA_DIR=/cosma5/data/durham/dc-mora2/surveys/
export MACAROON_DIR=/cosma/home/durham/dc-mora2/macaroons/

export DDF_PIPELINE_CLUSTER=spider
export LINC_DATA_DIR=/project/lofarvlbi/Share/surveys
export MACAROON_DIR=
'''


cluster = os.getenv('DDF_PIPELINE_CLUSTER')
basedir = os.getenv('LINC_DATA_DIR')
procdir = os.path.join(basedir,'processing')
macaroon_dir = os.getenv('MACAROON_DIR')
macaroon = glob.glob(os.path.join(macaroon_dir,'*lofarvlbi_upload.conf'))[0]


download_thread=None
download_name=None
stage_thread=None
stage_name=None
unpack_thread=None
unpack_name=None
#tidy_thread=None
#upload_name=None
#upload_thread=None
#operation='DynSpecMS'
totallimit=20
staginglimit=2

'''
updated in MySQL_utils.py:
update_status
get_lbcalibrator
set_lbcalibrator
'''

def update_status(name,status,stage_id=None,time=None,workdir=None,av=None,survey=None):
    # adapted from surveys_db
    # utility function to just update the status of a field
    # name can be None (work it out from cwd), or string (field name)
    id=name 
    with SurveysDB(survey=survey) as sdb:
        idd=sdb.db_get('lb_calibrators',id)
        if idd is None:
          raise RuntimeError('Unable to find database entry for field "%s".' % id)
        idd['status']=status
        tag_field(sdb,idd,workdir=workdir)
        if time is not None and idd[time] is None:
            idd[time]=datetime.datetime.now()
        if stage_id is not None:
            idd['staging_id']=stage_id
        sdb.db_set('lb_calibrators',idd)
    sdb.close()        

##############################
## staging

def stage_cal( id, srmpath='https://public.spider.surfsara.nl/project/lofarvlbi/srmlists/' ):
    srmfilename = id + '_srms.txt'
    response = requests.get(os.path.join(srmpath,srmfilename))
    data = response.text
    uris = data.rstrip('\n').split('\n')
    stage_id = stager_access.stage(uris)
    update_status(id, 'Staging', stage_id=stage_id )

def do_stage(field):
    update_status(field,'Staging')
    success=True
    try:
        stage_field(field,basedir+'/'+field,verbose=True)
    except RuntimeError:
        success=False
    if success:
        update_status(field,operation,'Staged')
    else:
        update_status(field,operation,'Stage failed')

##############################
## downloading

############# NEED TO CHANGE TO USE MACAROONS

def do_download( id ):
    update_status(id,'Downloading')
    ## get the staging id from the surveys database
    with SurveysDB(readonly=True) as sdb:
        idd=sdb.db_get('lb_calibrators',id)
        stage_id = idd['staging_id']
    sdb.close()

    ## create a directory and change to it
    cdir = os.getcwd()

    ## get the surls from the stager API
    surls = stager_access.get_surls_online(stage_id)
    project = surls[0].split('/')[-3]
    obsid = surls[0].split('/')[-2]
    obsid_path = os.path.join(project,obsid)
    if len(surls) > 0:
        caldir = os.path.join(str(os.getenv('LINC_DATA_DIR')),str(id))
        os.makedirs(caldir,exist_ok=True)
        os.chdir(caldir)
        ### NEED TO REPLACE THIS BIT WITH RCLONE STUFF OR GRID CERTIFICATE
        if 'juelich' in surls[0]:
            for surl in surls:
                dest = os.path.basename(surl)
                os.system('gfal-copy {:s} {:s}'.format(surl.replace('srm://lofar-srm.fz-juelich.de:8443','gsiftp://lofar-gridftp.fz-juelich.de:2811'),dest))
        if 'psnc' in surls[0]:
            for surl in surls:
                dest = os.path.basename(surl)
                os.system('gfal-copy {:s} {:s}'.format(surl.replace('srm://lta-head.lofar.psnc.pl:8443','gsiftp://gridftp.lofar.psnc.pl:2811'),dest))
        if 'sara' in surls[0]:
            ## can use a macaroon
            files = [ os.path.basename(val) for val in surls ]
            macaroon_dir = os.getenv('MACAROON_DIR')        
            lta_macaroon = glob.glob(os.path.join(macaroon_dir,'*LTA.conf'))[0]
            rc = Rclone( lta_macaroon, debug=True )
            rc.get_remote()
            d = rc.multicopy(rc.remote+os.path.join(obsid_path),files,caldir)
            if d['err'] or d['code']!=0:
                update_status(field,'rclone failed')
                print('Rclone failed for field {:s}'.format(field))
        os.chdir(cdir)
        ## check that everything was downloaded
        tarfiles = glob.glob(os.path.join(caldir,'*tar'))
        if len(tarfiles) == len(surls):
            print('Download successful for {:s}'.format(id) )
            update_status(id,'Downloaded',stage_id=0)
    else:
        print('SURLs do not appear to be online for {:s} (staging id {:s})'.format(id,str(stage_id)))
        update_status(id,'Download failed')

##############################
## unpacking

def do_unpack(field):
    update_status(field,'Unpacking')
    success=True
    cdir = os.getcwd()
    caldir = os.path.join(str(os.getenv('LINC_DATA_DIR')),field)
    os.chdir(caldir)
    ## get the tarfiles
    tarfiles = glob.glob('*tar')
    for trf in tarfiles:
        os.system( 'tar -xvf {:s}'.format(trf) )
    ## check that everything unpacked
    msfiles = glob.glob('L*MS')
    if len(msfiles) == len(tarfiles):
        update_status(field,'Unpacked')
        os.system('rm *.tar')
    else:
        update_status(field,'Unpack failed')
    os.chdir(cdir)

def unpack_tarfiles(field):
    cdir = os.getcwd()
    caldir = os.path.join(str(os.getenv('LINC_DATA_DIR')),str(field))
    os.chdir(caldir)
    tarfiles = glob.glob('SRMF*tar')
    ## need to rename them - this loop can be deleted once no longer using html
    for tf in tarfiles:
        tmp = tf.split('%2FL')[-1]
        os.system('mv {:s} {:s}'.format(tf, tmp))
        os.system('rm {:s}'.format(tf))
    tarfiles = glob.glob('*.tar')
    for tf in tar:
        os.system('tar xvf {:s}'.tf)
    ## check that everything is ok
    msfiles = glob.glob('*MS')
    if len(msfiles) == len(tarfiles):
        os.system('rm *.tar')
        success = True
    else:
        success = False
    os.chdir(cdir)
    return success

##############################
## verifying

def check_field(field):
    cdir = os.getcwd()
    procdir = os.path.join(str(os.getenv('LINC_DATA_DIR')),'processing')
    outdir = os.path.join(procdir,field)
    os.chdir(outdir)
    os.system('rm -rf tmp*')
    if os.path.isfile('finished.txt'):
        if os.path.isfile('cal_solutions.h5'):
            os.system('tar cvzf {:s}.tgz inspection *.json cal_solutions.h5'.format(field))
            success = True
        else:
            success = False
    else:
        success = False
    os.chdir(cdir)
    return success

''' Logic is as follows:

1. if there is a not started dataset, first operation is always to stage a dataset (NB a different operation if it's on rclone or on SDR -- can do SDR first). At most one staging thread. Set status to Staged on sucessful complete. From this point on we only look for datasets that are associated with the local cluster.

2. any Staged dataset can be downloaded. At most one download thread (which uses prepare_field): set status to Downloaded on successful complete

3. any Downloaded dataset can be unpacked. Set status to Unpacked when done (also uses prepare_field).

4. any Unpacked dataset can have the processing script run on it. Set status to Started on start. (status set to Verified on upload)

5. any Verified dataset can have the tidy up script run on it.
'''

while True:

    with SurveysDB(readonly=True) as sdb:
        sdb.cur.execute('select * from lb_calibrators where clustername="'+cluster+'" order by priority,id')
        result=sdb.cur.fetchall()
        sdb.cur.execute('select * from lb_calibrators where status="Not started" and priority>0 order by priority,id')
        result2=sdb.cur.fetchall()
        if len(result2)>0:
            nextfield=result2[0]['id']
        else:
            nextfield=None

        d={} ## len(fd)
        fd={}  ## dictionary of fields of a given status type
        for r in result:
            status=r['status']
            if status in d:
                d[status]=d[status]+1
                fd[status].append(r['id'])
            else:
                d[status]=1
                fd[status]=[r['id']]
        d['Not started']=len(result2)
        print('\n\n-----------------------------------------------\n\n')
        print('LB calibrator status on cluster %s' % (cluster))
        print(datetime.datetime.now())
        print()
        failed=0
        for k in sorted(d.keys()):
            print("%-20s : %i" % (k,d[k]))
            if 'ailed' in k:
                failed+=d[k]

        print()
        ksum=(len(glob.glob(basedir+'/*'))-4)-failed
        if ksum<0: ksum=0
        print(ksum,'live directories out of',totallimit)
        print('Next field to work on is',nextfield)

        if download_thread is not None:
            print('Download thread is running (%s)' % download_name)
        if unpack_thread is not None:
            print('Unpack thread is running (%s)' % unpack_name)
        if stage_thread is not None:
            ## can be more clever about this and have thread terminate so able to start another one and then 
            print('Stage thread is running (%s)' % stage_name)

        if download_thread is not None and not download_thread.is_alive():
            print('Download thread seems to have terminated')
            download_thread=None

        if unpack_thread is not None and not unpack_thread.is_alive():
            print('Unpack thread seems to have terminated')
            unpack_thread=None

        if stage_thread is not None and not stage_thread.is_alive():
            print('Stage thread seems to have terminated')
            stage_thread=None

        ## need to start staging if: staging isn't happening -or- staging is happening but less than staging limit
        if 'Staging' in d.keys():
            nstage = d['Staging']
        else:
            nstage = 0

        if nstage < staginglimit and nextfield is not None and 'Staged' not in d:
            stage_name=nextfield
            print('We need to stage a new field (%s)' % stage_name)
            stage_thread=threading.Thread(target=stage_cal,args=(stage_name,))
            stage_thread.start()

        if 'Staging' in d.keys():
            ## get the staging ids and then check if they are complete
            ## loop over ids and call database to get staging id
            for field in fd['Staging']:
                ## get the stage id
                r = [ item for item in result if item['id'] == field ][0]
                s = r['staging_id']
                stage_status = stager_access.get_status(s)
                #    “new”, “scheduled”, “in progress”, “aborted”, “failed”, “partial success”, “success”, “on hold” 
                if stage_status == 'success':
                    print('Staging for {:s} is complete, updating status'.format(str(r['staging_id'])))
                    update_status(r['id'],'Staged') ## don't reset the staging id till download happens
                else:
                    print('Staging for {:s} is {:s}'.format(field,stage_status))

        ## this does one download at a time
        if ksum<totallimit and 'Staged' in d and download_thread is None:
            download_name=fd['Staged'][0]
            print('We need to download a new file (%s)!' % download_name)
            ## probably want to pass the staging id here
            download_thread=threading.Thread(target=do_download, args=(download_name,))
            download_thread.start()

        ## unpacking the files
        if 'Downloaded' in d and unpack_thread is None:
            unpack_name=fd['Downloaded'][0]
            print('We need to unpack a new file (%s)!' % unpack_name)
            unpack_thread=threading.Thread(target=do_unpack, args=(unpack_name,))
            unpack_thread.start()

        if 'Unpacked' in d:
            for field in fd['Unpacked']:
                print('Running a new job',field)
                update_status(field,"Queued")
                ### will need to change the script
                command="sbatch -J %s %s/slurm/run_linc_calibrator.sh %s" % (field, str(basedir).rstrip('/'), field)
                if os.system(command):
                    update_status(field,"Submission failed")

        if 'Queued' in d:
            for field in fd['Queued']:
                ## need some sort of file to be written at the end
                print('Verifying processing for',field)
                result = check_field(field)
                if result:
                    update_status(field,'Verified')
                else:
                    update_status(field,'Workflow failed')

        ## this will also need to be changed to use macaroons to copy back to spider
        if 'Verified' in d:
            for field in fd['Verified']:
                ## use rclone / macaroon to copy the tgz file
                tarfile = glob.glob(os.path.join(procdir,field)+'/*tgz')[0]
                rc = RClone( macaroon, debug=True )
                rc.get_remote()
                d = rc.execute_live(['-P', 'copy', tarfile]+[rc.remote + '/' + 'disk/surveys/'])
                if d['err'] or d['code']!=0:
                    update_status(field,'rclone failed')
                    print('Rclone failed for field {:s}'.format(field))
                else:
                    print('Tidying uploaded directory for',field)
                    update_status(field,'Complete')
                    ## delete the directory
                    os.system( 'rm -r {:s}'.format(os.path.join(procdir,field)))
                    ## delete the initial data
                    os.system( 'rm -r {:s}'.format(os.path.join(basedir,field)))

        print('\n\n-----------------------------------------------\n\n')
        
    sleep(60)
