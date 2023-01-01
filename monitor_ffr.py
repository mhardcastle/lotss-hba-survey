#!/usr/bin/python

# clone of monitor.py to do monitoring of full field reprocessing

from __future__ import print_function
from time import sleep
import datetime
from surveys_db import SurveysDB
from run_full_field_reprocessing_pipeline import update_status,stage_field
from reprocessing_utils import prepare_field
import os
import threading

import glob

cluster=os.environ['DDF_PIPELINE_CLUSTER']
download_thread=None
download_name=None
stage_thread=None
stage_name=None
#tidy_thread=None
#upload_name=None
#upload_thread=None
basedir='/beegfs/car/mjh/DS'
operation='DynSpecMS'
totallimit=20

def do_download(field):
    update_status(field,operation,'Downloading')
    success=True
    try:
        prepare_field(field,basedir+'/'+field,verbose=True)
    except RuntimeError:
        success=False
    if success:
        update_status(field,operation,'Downloaded')
    else:
        update_status(field,operation,'Download failed')

def do_stage(field):
    update_status(field,operation,'Staging')
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

3. any Downloaded dataset can have the processing script run on it. Set status to Started on start. (status set to Verified on upload)

4. any Verified dataset can have the tidy up script run on it.
'''

while True:

    with SurveysDB(readonly=True) as sdb:
        sdb.cur.execute('select * from full_field_reprocessing where operation="'+operation+'" and clustername="'+cluster+'" order by priority desc,id')
        result=sdb.cur.fetchall()
        sdb.cur.execute('select * from full_field_reprocessing where operation="'+operation+'" and status="Not started" and priority>0 order by priority desc,id')
        result2=sdb.cur.fetchall()
        if len(result2)>0:
            nextfield=result2[0]['id']
        else:
            nextfield=None

    d={}
    fd={}
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
    print('Full-field reprocessing (%s) status on cluster %s' % (operation,cluster))
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
    if stage_thread is not None:
        print('Stage thread is running (%s)' % stage_name)
        
    if download_thread is not None and not download_thread.isAlive():
        print('Download thread seems to have terminated')
        download_thread=None

    if stage_thread is not None and not stage_thread.isAlive():
        print('Stage thread seems to have terminated')
        stage_thread=None

    if stage_thread is None and nextfield is not None and 'Staged' not in d:
        # We only need one field staged at a time
        stage_name=nextfield
        print('We need to stage a new field (%s)' % stage_name)
        update_status(stage_name,operation,'Staging')
        stage_thread=threading.Thread(target=do_stage,args=(stage_name,))
        stage_thread.start()
        
    if ksum<totallimit and 'Staged' in d and download_thread is None:
        download_name=fd['Staged'][0]
        print('We need to download a new file (%s)!' % download_name)
        download_thread=threading.Thread(target=do_download, args=(download_name,))
        download_thread.start()

    if 'Downloaded' in d:
        for field in fd['Downloaded']:
            print('Running a new job',field)
            command="qsub -v FIELD=%s -N reprocessing-%s /home/mjh/pipeline-master/lotss-hba-survey/torque/dynspec.qsub" % (field, field)
            if os.system(command)==0:
                update_status(field,operation,"Queued")

    # this code does not work but is a placeholder for tidy-up code to be implemented
    if 'Uploaded' in d:
        for r in result:
            if r['vlow_image']=='Uploaded':
                field=r['id']
                print('Tidying uploaded directory for',field)
                target='/data/lofar/DR2/fields/'+field
                for f in ['image_full_vlow_nocut_m.app.restored.fits','image_full_vlow_nocut_m.int.restored.fits','WSCLEAN_low-MFS-image.fits','WSCLEAN_low-MFS-image-int.fits']:
                    command='cp '+basedir+'/'+field+'/'+f+' '+target
                    print('running',command)
                    os.system(command)
                command='rm -r '+basedir+'/'+field
                os.system(command)
                update_status(field,'Archived')

    print('\n\n-----------------------------------------------\n\n')
        
    sleep(60)
