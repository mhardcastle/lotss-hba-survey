#!/usr/bin/python

# clone of monitor.py to do monitoring of full field reprocessing

from __future__ import print_function
from time import sleep
import datetime
from surveys_db import SurveysDB, tag_field

## need to update this
from run_full_field_reprocessing_pipeline import stage_field
## from reprocessing_utils import prepare_field  ## ddf-pipeline utils
import os
import threading
import glob
import requests
import staging_access


#################################
## CLUSTER SPECIFICS - config file
cluster = 'cosma'
basedir = '/cosma5/data/durham/dc-mora2/surveys/'


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

def stage_cal( srmfilename, srmpath='https://public.spider.surfsara.nl/project/lofarvlbi/srmlists/' ):
    response = requests.get(os.path.join(srmpath,srmfilename))
    data = response.text
    uris = data.rstrip('\n').split('\n')
    stage_id = stager_access.stage(uris)
    update_status(srmfilename.replace('_srms.txt',''), 'Staging', stage_id=stage_id )
    return stage_id

def check_stage( stage_id, srmfilename ):
    ## there is actually a function in the stager_access module to do this ... 
    stager_access.get_progress()
get_surls_pending

    status = 'new'
    srmfile = open(srmfilename)
    uris = srmfile.read().splitlines()
    while 'success' not in status:
        status = sa.get_status(stage_id)
        numonline = sa.get_surls_online(stage_id)
        print('Status',status,'num total',len(uris),'num staged',len(numonline),srmfilename)
        if float(len(numonline)) > 0.99*float(len(uris)):
            break
        time.sleep(60)



def do_rclone_download(cname,f,verbose=False):
    '''
    Download required data from field cname into location f
    '''
    #tarfiles=['images.tar','uv.tar']
    for macaroon, directory in [('maca_sksp_tape_DR2_readonly.conf',''),('maca_sksp_tape_DDF.conf','archive/'),('maca_sksp_tape_DDF_readonly.conf','other/')]:



        try:
            rc=RClone(macaroon,debug=True)
        except RuntimeError:
            print('Macaroon',macaroon,'does not exist!')
            continue
        rc.get_remote()
        files=rc.get_files(directory+cname)
        tarfiles=[fl for fl in files if 'images' in fl or 'uv' in fl]
        if tarfiles:
            d=rc.multicopy(rc.remote+directory+cname,tarfiles,f)
            if d['err'] or d['code']!=0:
                continue
        else:
            continue
        break
        
    else:
        raise RuntimeError('Failed to download from any source')
    tarfiles = glob.glob('*tar')
    untar(f,tarfiles,verbose=verbose)



        # Format of lines is like: srm://srm.grid.sara.nl:8443/pnfs/grid.sara.nl/data/lofar/ops/projects/lt10_010/775619/L775619_SB055_uv.MS_76a2361e.tar
        startdir = os.getcwd()
        os.chdir(outdir)
        for line in srmfile:
                origline = line[:-1]
                line = origline.split('/')
                print(line)
                projectcode = line[-3]
                obsid = line[-2]
                fileid = line[-1]
                if 'psnc' in origline:
                        location = 'Poznan'
                if 'juelich' in origline:
                        location = 'Juelic'
                if 'sara' in origline:
                        location = 'Sara'
                if os.path.exists(fileid.split('_')[0] + '_' + fileid.split('_')[1] + '_'  + fileid.split('_')[2]):
                        print('Already downloaded  ',fileid)
                        continue
                if os.path.exists(fileid.split('_')[0] + '_' + fileid.split('_')[1] + '_'  + fileid.split('_')[2].replace('.MS','_avg.MS')):
                        print('Already downloaded  and averaged',fileid)
                        continue

                print('Downloading %s'%fileid)
                if location != 'Poznan':
                        print('rclone  --multi-thread-streams 1 --config=/project/lotss/Software/prefactor-operations/macaroons/maca_sksp_LTA.conf copy maca_sksp_LTA:/%s/%s/%s  ./'%(projectcode,obsid,fileid))
                        os.system('rclone  --multi-thread-streams 1 --config=/project/lotss/Software/prefactor-operations/macaroons/maca_sksp_LTA.conf copy maca_sksp_LTA:/%s/%s/%s  ./'%(projectcode,obsid,fileid))
                else:
                        print('singularity run -B /etc/grid-security/certificates,/project /cvmfs/atlas.cern.ch/repo/containers/fs/singularity/x86_64-centos7 gfal-copy gsiftp://gridftp.lofar.psnc.pl:2811/lofar/ops/projects/%s/%s/%s ./'%(projectcode,obsid,fileid))
                        os.system('singularity run -B /etc/grid-security/certificates,/project /cvmfs/atlas.cern.ch/repo/containers/fs/singularity/x86_64-centos7 gfal-copy gsiftp://gridftp.lofar.psnc.pl:2811/lofar/ops/projects/%s/%s/%s ./'%(projectcode,obsid,fileid))


                print('Unpacking %s'%fileid)
                os.system('tar -xf %s'%fileid)
                if compress:
                        msfilename = fileid.split('_')[0] + '_' + fileid.split('_')[1] + '_'  + fileid.split('_')[2]
                        compress_average(msfilename)
                os.system('rm %s'%fileid)
        os.chdir(startdir)
        return



def download_cal( id ):
    ## adapted (loosely) from tim's scripts 
    ## get the srmlist
    srmfilename = id+'_srms.txt'
    ## start staging
    stage_id = stage_cal( srmfilename )
    ## check the status - this should finish when staging is done
    check_stage( stageid, srmfilename )
    ## now download the data





def unpack_cal(  ):


print('Cal download status',cal_download_status)
## stage the data if not and then download
if cal_download_status == False:
    cal_stage_id = stage_data_non_sara(calsrm)
    print('Checking calibrator %s is staged'%fieldproperties['calib_OBSID'])
    check_stage_non_sara(calsrm,cal_stage_id)
    print('Downloading calibrator %s'%fieldproperties['calib_OBSID'])
    download_data(calsrm,outcaldir,False)

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

def do_download(field):
    update_status(field'Downloading')
    success=True
    try:
        ## function that downloads, end result is tar files on disk
        prepare_field(field,basedir+'/'+field,verbose=True,operations=['download'])
    except RuntimeError:
        success=False
    if success:
        update_status(field,'Downloaded')
    else:
        update_status(field,'Download failed')

def do_unpack(field):
    update_status(field,operation,'Unpacking')
    success=True
    try:
        ## function that unpacks tar files, end result is MS on disk
        success=prepare_field(field,basedir+'/'+field,verbose=True,operations=['untar','fixsymlinks','makelist'])
    except RuntimeError:
        success=False
    if success:
        update_status(field,operation,'Unpacked')
    else:
        update_status(field,operation,'Unpack failed')

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
        
os.chdir(basedir)

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
        sid={}
        for r in result:
            status=r['status']
            if status in d:
                d[status]=d[status]+1
                fd[status].append(r['id'])
            else:
                d[status]=1
                fd[status]=[r['id']]
            if status == 'Staging':
                sid[r['staging_id']] = r['id']
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
        ksum=len(glob.glob(basedir+'/*'))-failed
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

        if download_thread is not None and not download_thread.isAlive():
            print('Download thread seems to have terminated')
            download_thread=None

        if unpack_thread is not None and not unpack_thread.isAlive():
            print('Unpack thread seems to have terminated')
            unpack_thread=None

        if stage_thread is not None and not stage_thread.isAlive():
            print('Stage thread seems to have terminated')
            stage_thread=None

        if d['Staging'] < staginglimit and nextfield is not None and 'Staged' not in d:
            stage_name=nextfield
            print('We need to stage a new field (%s)' % stage_name)
            stage_thread=threading.Thread(target=stage_cal,args=(stage_name,))
            stage_thread.start()

        if d['Staging'] > 0:
            ## get the staging ids and then check if they are complete
            ## loop over ids and call database to get staging id
            for s in sid:
                pending = stager_access.get_surls_pending(s)
                if len(pending) = 0:
                    update_status(sid[s],'Staged',stage_id=0)
        
        if ksum<totallimit and 'Staged' in d and download_thread is None:
            download_name=fd['Staged'][0]
            print('We need to download a new file (%s)!' % download_name)
            download_thread=threading.Thread(target=do_download, args=(download_name,))
            download_thread.start()

        if 'Downloaded' in d and unpack_thread is None:
            unpack_name=fd['Downloaded'][0]
            print('We need to unpack a new file (%s)!' % unpack_name)
            unpack_thread=threading.Thread(target=do_unpack, args=(unpack_name,))
            unpack_thread.start()

        if 'Unpacked' in d:
            for field in fd['Unpacked']:
                print('Running a new job',field)
                update_status(field,operation,"Queued")
                ### will need to change the script
                command="qsub -v FIELD=%s -N repro-%s /home/mjh/pipeline-master/lotss-hba-survey/torque/dynspec.qsub" % (field, field)
                if os.system(command):
                    update_status(field,operation,"Submission failed")

        ## need to step here where we check that if it's done, the pipeline run was successful. 

        ## this will also need to be changed to use macaroons to copy back to spider
        if 'Verified' in d:
            for field in fd['Verified']:
                print('Tidying uploaded directory for',field)
                target='/data/lofar/DR2/fields/'+field
                g=glob.glob(basedir+'/'+field+'/*.tgz')
                for f in g:
                    command='cp '+f+' '+target
                    print('running',command)
                    os.system(command)
                command='rm -r '+basedir+'/'+field
                print('running',command)
                os.system(command)
                update_status(field,operation,'Complete')

        print('\n\n-----------------------------------------------\n\n')
        
    sleep(60)
