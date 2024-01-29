#!/usr/bin/python

# clone of monitor.py to do monitoring of full field reprocessing

from __future__ import print_function
from time import sleep
import datetime
from surveys_db import SurveysDB, tag_field, get_cluster
from calibrator_utils import check_int_stations

## need to update this
#from run_full_field_reprocessing_pipeline import stage_field
#from reprocessing_utils import prepare_field  ## ddf-pipeline utils
import os
import threading
import glob
import requests
import stager_access
from rclone import RClone   ## DO NOT pip3 install --user python-rclone -- use https://raw.githubusercontent.com/mhardcastle/ddf-pipeline/master/utils/rclone.py
from download_file import download_file
import numpy as np

#################################
## CLUSTER SPECIFICS - use environment variables

'''
export DDF_PIPELINE_CLUSTER=cosma
export LINC_DATA_DIR=/cosma5/data/durham/dc-mora2/surveys/
export MACAROON_DIR=/cosma/home/durham/dc-mora2/macaroons/

export DDF_PIPELINE_CLUSTER=spider
export LINC_DATA_DIR=/project/lofarvlbi/Share/surveys
export MACAROON_DIR=/home/lofarvlbi-lmorabito/macaroons/

export DDF_PIPELINE_CLUSTER=galahad
export LINC_DATA_DIR=
export MACAROON_DIR=

export NO_GRID=True if you don't want to use grid tools
export USE_TORQUE=True to use Torque rather than Slurm scripts

'''

user = os.getenv('USER')
home = os.getenv('HOME')
if len(user) > 20:
    user = user[0:20]
cluster = os.getenv('DDF_PIPELINE_CLUSTER')
basedir = str(os.getenv('LINC_DATA_DIR'))
procdir = os.path.join(basedir,'processing')


download_thread=None
download_name=None
stage_thread=None
stage_name=None
unpack_thread=None
unpack_name=None
verify_thread=None
verify_name=None
totallimit=20
staginglimit=2
maxstaged=6

## cluster specific queuing limits
if cluster == 'spider':
    maxqueue = 10
elif cluster == 'cosma':
    maxqueue = 5
else:
    # default!
    maxqueue = 10
    
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

##############################
## downloading

def do_download( id ):
    update_status(id,'Downloading')
    ## get the staging id from the surveys database
    with SurveysDB(readonly=True) as sdb:
        idd=sdb.db_get('lb_calibrators',id)
        stage_id = idd['staging_id']

    ## get the surls from the stager API
    surls = stager_access.get_surls_online(stage_id)
    project = surls[0].split('/')[-3]
    obsid = surls[0].split('/')[-2]
    obsid_path = os.path.join(project,obsid)
    if len(surls) > 0:
        caldir = os.path.join(basedir,str(id))
        if not os.path.isdir(caldir):
            os.makedirs(caldir)
        if 'juelich' in surls[0]:
            print('Juelich download:',surls[0])
            if 'NO_GRID' in os.environ:
                logfile=None
                prefix="https://lofar-download.fz-juelich.de/webserver-lofar/SRMFifoGet.py?surl="
                for surl in surls:
                    dest = os.path.join(caldir,os.path.basename(surl))
                    if not os.path.isfile(dest):
                        download_file(prefix+surl,dest,retry_partial=True,progress_bar=True,retry_size=1024)
                    else:
                        print(dest,'exists already, not downloading')
                pass
            else:
                logfile = '{:s}_gfal.log'.format(id)
                for surl in surls:
                    dest = os.path.join(caldir,os.path.basename(surl))
                    os.system('gfal-copy {:s} {:s} > {:s} 2>&1'.format(surl.replace('srm://lofar-srm.fz-juelich.de:8443','gsiftp://lofar-gridftp.fz-juelich.de:2811'),dest,logfile))
        elif 'psnc' in surls[0]:
            print('Poznan download...')
            logfile = '{:s}_wget.log'.format(id)
            with open(os.path.join(caldir,'html.txt'),'w') as f:
                for surl in surls:
                    f.write('https://lta-download.lofar.psnc.pl/lofigrid/SRMFifoGet.py?surl={:s}\n'.format(surl))
            f.close()
            os.system('wget -i {:s} --no-check-certificate -P {:s} > {:s} 2>&1'.format(os.path.join(caldir,'html.txt'),caldir,logfile))
            files = glob.glob(os.path.join(caldir,'SRMF*tar'))
            for ff in files:
                tmp = ff.split('%2F')[-1]
                os.system('mv {:s} {:s}'.format(ff,os.path.join(caldir,tmp)))
        elif 'sara' in surls[0]:
            print('SARA download...')
            logfile = '{:s}_rclone.log'.format(id)
            ## can use a macaroon
            files = [ os.path.basename(val) for val in surls ]
            macaroon_dir = os.getenv('MACAROON_DIR')        
            lta_macaroon = glob.glob(os.path.join(macaroon_dir,'*LTA.conf'))[0]
            rc = RClone( lta_macaroon, debug=True )
            rc.get_remote()
            #d = rc.multicopy(rc.remote+obsid_path,files,caldir)
            for f in files:
                d = rc.execute(['-P','copy',rc.remote + os.path.join(obsid_path,f)]+[caldir]) 
            if d['err'] or d['code']!=0:
                update_status(id,'rclone failed')
                print('Rclone download failed for field {:s}'.format(id))
                with open(logfile,'w') as f:
                    f.write('Rclone failed for field {:s}'.format(id))
            else:
                with open(logfile,'w') as f:
                    f.write('Rclone finished successfully for field {:s}\n'.format(id))                
        else:
            raise RuntimeError('Cannot work out what to do with SURL!')
        ## check that everything was downloaded
        tarfiles = glob.glob(os.path.join(caldir,'*tar'))
        if len(tarfiles) == len(surls):
            print('Download successful for {:s}'.format(id) )
            update_status(id,'Downloaded',stage_id=0)
            if logfile and os.path.exists(logfile):
                os.system('rm {:s}'.format(logfile))
        else:
            ## find what hasn't downloaded
            trfs = [ os.path.basename(trf) for trf in tarfiles ]
            not_downloaded = [ surl for surl in surls if os.path.basename(surl) not in trfs ]
            os.system('echo Number of files downloaded does not match number staged >> {:s}'.format(logfile))
            if 'juelich' in not_downloaded[0]:
                for surl in not_downloaded:
                    dest = os.path.join(caldir,os.path.basename(surl))
                    os.system('gfal-copy {:s} {:s} > {:s} 2>&1'.format(surl.replace('srm://lofar-srm.fz-juelich.de:8443','gsiftp://lofar-gridftp.fz-juelich.de:2811'),dest,logfile))
            tarfiles = glob.glob(os.path.join(caldir,'*tar'))
            if len(tarfiles) == len(surls):
                print('Download successful for {:s}'.format(id) )
                update_status(id,'Downloaded',stage_id=0)
                if os.path.exists(logfile):
                    os.system('rm {:s}'.format(logfile))
            else:
                os.system('echo Attempt to re-download failed >> {:s} 2>&1'.format(logfile))
    else:
        print('SURLs do not appear to be online for {:s} (staging id {:s})'.format(id,str(stage_id)))
        update_status(id,'Download failed')

##############################
## unpacking

def do_unpack(field):
    update_status(field,'Unpacking')
    success=True
    caldir = os.path.join(basedir,field)
    ## get the tarfiles
    tarfiles = glob.glob(os.path.join(caldir,'*tar'))
    for trf in tarfiles:
        os.system( 'cd {:s}; tar -xvf {:s} >> {:s}_unpack.log 2>&1'.format(caldir,trf,field) )

    ## check that everything unpacked
    msfiles = glob.glob('{:s}/L*MS'.format(caldir))
    if len(msfiles) == len(tarfiles):
        update_status(field,'Unpacked')
        os.system('rm {:s}/*.tar'.format(caldir))
        os.system('rm {:s}_unpack.log'.format(field))
    else:
        update_status(field,'Unpack failed')

##############################
## verifying

def check_field(field):
    outdir = os.path.join(procdir,field)
    solutions=os.path.join(outdir,'cal_solutions.h5')
    if os.path.isfile(solutions):
        with SurveysDB() as sdb:
            idd=sdb.db_get('lb_calibrators',field)
            d=check_int_stations(solutions)
            for k in d:
                idd[k]=d[k]
            if 'err' not in d:
                idd['err']=np.nan # horrible feature of SurveysDB; force NULL to be written
            sdb.db_set('lb_calibrators',idd)
        success=not(os.system('cd {:s}; tar cvzf {:s}.tgz {:s}/inspection {:s}/*.json {:s}/cal_solutions.h5'.format(procdir,field,field,field,field)))
        if success:
            os.system('rm -rf {:s}/tmp*'.format(outdir))
    else:
        success = False
    return success

def do_verify(field):
    tarfile = glob.glob(procdir+'/'+field+'.tgz')[0]
    macaroon_dir = os.getenv('MACAROON_DIR')
    macaroon = glob.glob(os.path.join(macaroon_dir,'*lofarvlbi.conf'))[0]
    rc = RClone( macaroon, debug=True )
    rc.get_remote()
    d = rc.execute_live(['-P', 'copy', tarfile]+[rc.remote + '/' + 'disk/surveys/'])
    if d['err'] or d['code']!=0:
        update_status(field,'rclone failed')
        print('Rclone failed for field {:s}'.format(field))
    else:
        print('Tidying uploaded directory for',field)
        update_status(field,'Complete',time='end_date')
        ## delete the directory
        os.system( 'rm -r {:s}'.format(os.path.join(procdir,field)))
        ## delete the initial data
        os.system( 'rm -r {:s}'.format(os.path.join(basedir,field)))
        ## delete the tarfile
        os.system( 'rm '+tarfile )

''' Logic is as follows:

1. if there is a not started dataset, first operation is always to stage a dataset (NB a different operation if it's on rclone or on SDR -- can do SDR first). At most one staging thread. Set status to Staged on sucessful complete. From this point on we only look for datasets that are associated with the local cluster.

2. any Staged dataset can be downloaded. At most one download thread (which uses prepare_field): set status to Downloaded on successful complete

3. any Downloaded dataset can be unpacked. Set status to Unpacked when done (also uses prepare_field).

4. any Unpacked dataset can have the processing script run on it. Set status to Started on start. (status set to Verified on upload)

5. any Verified dataset can have the tidy up script run on it.
'''

while True:

    with SurveysDB(readonly=True) as sdb:
        sdb.cur.execute('select * from lb_calibrators where clustername="'+cluster+'" and username="'+user+'" order by priority,id')
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
        print('Stage thread is running (%s)' % stage_name)
    if verify_thread is not None:
        print('Verify thread is running (%s)' % verify_name)

    if download_thread is not None and not download_thread.is_alive():
        print('Download thread seems to have terminated')
        download_thread=None

    if unpack_thread is not None and not unpack_thread.is_alive():
        print('Unpack thread seems to have terminated')
        unpack_thread=None

    if stage_thread is not None and not stage_thread.is_alive():
        print('Stage thread seems to have terminated')
        stage_thread=None

    if verify_thread is not None and not verify_thread.is_alive():
        print('Verify thread seems to have terminated')
        verify_thread=None

    ## need to start staging if: staging isn't happening -or- staging is happening but less than staging limit
    if 'Staging' in d.keys():
        nstage = d['Staging']
    else:
        nstage = 0
    if 'Staged' in d.keys():
        nstaged = d['Staged']
    else:
        nstaged = 0
    if nstaged < maxstaged:
        if nstage <= staginglimit:
            do_stage = True
        else:
            do_stage = False
    else:
        do_stage = False

    if do_stage and nextfield is not None:
        stage_name=nextfield
        print('We need to stage a new field (%s)' % stage_name)
        stage_cal(stage_name)
        #stage_thread=threading.Thread(target=stage_cal,args=(stage_name,))
        #stage_thread.start()

    if 'Staging' in d.keys():
        ## get the staging ids and then check if they are complete
        ## loop over ids and call database to get staging id
        for field in fd['Staging']:
            ## get the stage id
            r = [ item for item in result if item['id'] == field ][0]
            s = r['staging_id']
            try:
                stage_status = stager_access.get_status(s)
            except Exception as e:
                stage_status=None
                print('Stager API reported exception',e)
                
            #    "new", "scheduled", "in progress", "aborted", "failed", "partial success", "success", "on hold" 
            if stage_status == 'success' or stage_status == 'completed':
                print('Staging for {:s} is complete, updating status'.format(str(r['staging_id'])))
                update_status(r['id'],'Staged') ## don't reset the staging id till download happens
            elif stage_status is not None:
                print('Staging for {:s} is {:s} (staging id {:s})'.format(field,stage_status,str(s)))

    ## this does one download at a time
    if ksum<totallimit and 'Staged' in d and download_thread is None and not os.path.isfile(home+'/.nocaldownload'):
        download_name=fd['Staged'][0]
        print('We need to download a new file (%s)!' % download_name)
        download_thread=threading.Thread(target=do_download, args=(download_name,))
        download_thread.start()

    ## unpacking the files
    if 'Downloaded' in d and unpack_thread is None:
        unpack_name=fd['Downloaded'][0]
        print('We need to unpack a new file (%s)!' % unpack_name)
        unpack_thread=threading.Thread(target=do_unpack, args=(unpack_name,))
        unpack_thread.start()

    if 'Unpacked' in d:
        if 'Queued' in d:
            nq = d['Queued']
        else:
            nq = 0
        for field in fd['Unpacked']:
            if nq < maxqueue:
                nq = nq + 1
                print('Running a new job',field)
                update_status(field,'Queued',time='start_date',workdir=os.path.join(basedir,str(field))
)
                if 'USE_TORQUE' in os.environ:
                    command="qsub -v OBSID=%s -N lbcal-%s %s/lotss-hba-survey/torque/run_calibrator.qsub" % (field, field, os.environ['DDF_DIR'] )
                else:
                    command="sbatch -J %s %s/slurm/run_linc_calibrator.sh %s" % (field, str(basedir).rstrip('/'), field)
                if os.system(command):
                    update_status(field,"Submission failed")
            else:
                print( 'Queue is full, {:s} waiting for submission'.format(field) )

    if 'Queued' in d:
        for field in fd['Queued']:
            print('Checking processing for',field)
            outdir = os.path.join(procdir,field)
            if os.path.isfile(os.path.join(outdir,'finished.txt')):        
                result = check_field(field)
                if result:
                    update_status(field,'Verified')
                else:
                    update_status(field,'Workflow failed')

    ## this will also need to be changed to use macaroons to copy back to spider
    if 'Verified' in d and verify_thread is None and not os.path.isfile(home+'/.noverify'):
        verify_name = fd['Verified'][0]
        verify_thread=threading.Thread(target=do_verify, args=(verify_name,))
        verify_thread.start()


    print('\n\n-----------------------------------------------\n\n')
    
    sleep(60)
