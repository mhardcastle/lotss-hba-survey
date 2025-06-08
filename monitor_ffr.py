#!/usr/bin/env python

# clone of monitor.py to do monitoring of full field reprocessing

from __future__ import print_function
from time import sleep
import datetime
from surveys_db import SurveysDB
from run_full_field_reprocessing_pipeline import update_status,stage_field
from reprocessing_utils import prepare_field
import os
import threading
from surveys_db import SurveysDB

import glob

home=os.environ['HOME']
cluster=os.environ['DDF_PIPELINE_CLUSTER']
download_thread=None
download_name=None
stage_threads={}
unpack_thread=None
unpack_name=None
#tidy_thread=None
#upload_name=None
#upload_thread=None
basedir='/beegfs/car/mjh/ffr'
operation=None # all operations
totallimit=30
stagelimit=4

def update_all_status(field,status):
    with SurveysDB() as sdb:
        sdb.cur.execute('select * from full_field_reprocessing where id=%s and status!="Verified"',(field,))
        results=sdb.cur.fetchall()
    for r in results:
        update_status(field,r['operation'],status)

def do_download(field):
    update_all_status(field,'Downloading')
    success=True
    try:
        prepare_field(field,basedir+'/'+field,verbose=True)
    except RuntimeError:
        success=False
    if success:
        update_all_status(field,'Downloaded')
    else:
        update_all_status(field,'Download failed')

def do_unpack(field):
    update_all_status(field,'Unpacking')
    success=True
    try:
        success=prepare_field(field,basedir+'/'+field,verbose=True,operations=['untar','fixsymlinks','makelist'])
    except RuntimeError:
        success=False
    if success:
        update_all_status(field,'Unpacked')
    else:
        update_all_status(field,'Unpack failed')

def do_stage(field):
    update_all_status(field,'Staging')
    success=True
    try:
        stage_field(field,basedir+'/'+field,verbose=True)
    except RuntimeError:
        success=False
    if success:
        update_all_status(field,'Staged')
    else:
        update_all_status(field,'Stage failed')
        
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
        if operation is not None:
            sdb.cur.execute('select * from full_field_reprocessing where operation="'+operation+'" and clustername="'+cluster+'" order by priority desc,id')
            result=sdb.cur.fetchall()
            sdb.cur.execute('select * from full_field_reprocessing where operation="'+operation+'" and status="Not started" and priority>0 order by priority desc,id')
            result2=sdb.cur.fetchall()
        else:
            sdb.cur.execute('select * from full_field_reprocessing where clustername="'+cluster+'" order by priority desc,id')
            result=sdb.cur.fetchall()
            sdb.cur.execute('select distinct id from full_field_reprocessing where status="Not started" and priority>10 order by priority desc,id')
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
    if len(stage_threads)>0:
        for k in list(stage_threads.keys()):
            print('Stage thread is running (%s)' % k)
            if not stage_threads[k].is_alive():
                print('Stage thread seems to have terminated')
                del(stage_threads[k])

    if download_thread is not None and not download_thread.is_alive():
        print('Download thread seems to have terminated')
        download_thread=None

    #if stage_thread is not None and not stage_thread.is_alive():
    #    print('Stage thread seems to have terminated')
    #    stage_thread=None

    if len(stage_threads)<stagelimit and nextfield is not None and 'Staged' not in d:
        stage_name=nextfield
        print('We need to stage a new field (%s)' % stage_name)
        stage_thread=threading.Thread(target=do_stage,args=(stage_name,))
        stage_thread.start()
        stage_threads[stage_name]=stage_thread

    if 'Downloaded' in d and download_thread is None:
        for field in list(set(fd['Downloaded'])):
            print('Running a new job',field)
            update_all_status(field,"Queued")
            command="qsub -v FIELD=%s -N ffr-%s /home/mjh/pipeline-master/lotss-hba-survey/torque/ffr.qsub" % (field, field)
            if os.system(command):
                update_all_status(field,"Submission failed")

    if ksum<totallimit and 'Staged' in d and download_thread is None and not os.path.isfile(home+'/.nodownload'):
        download_name=fd['Staged'][0]
        print('We need to download a new field (%s)!' % download_name)
        download_thread=threading.Thread(target=do_download, args=(download_name,))
        download_thread.start()

    #if 'Verified' in d:
    #    for field in fd['Verified']:
    #        print('Tidying uploaded directory for',field)
    #        target='/data/lofar/DR2/fields/'+field
    #        g=glob.glob(basedir+'/'+field+'/*.tgz')
    #        for f in g:
    #            command='cp '+f+' '+target
    #            print('running',command)
    #            os.system(command)
    #        command='rm -r '+basedir+'/'+field
    #        print('running',command)
    #        os.system(command)
    #        update_status(field,operation,'Complete')

    print('\n\n-----------------------------------------------\n\n')
        
    sleep(60)
