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
export MACAROON_DIR=/home/lofarvlbi-lmorabito/macaroons/

export DDF_PIPELINE_CLUSTER=galahad
export LINC_DATA_DIR=
export MACAROON_DIR=
'''


cluster = os.getenv('DDF_PIPELINE_CLUSTER')
basedir = os.getenv('LINC_DATA_DIR')
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


def fix_unpack( ):

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

    for obsid in fd['Unpack failed']:
        caldir = basedir = os.path.join( os.getenv('LINC_DATA_DIR'), obsid )
        n_ms = len( glob.glob( os.path.join( caldir, 'L*MS' ) ) )
        n_tar = len( glob.glob( os.path.join( caldir, '*tar' ) ) )
        print( 'Obsid {:s} has {:s} failed subbands.'.format( obsid, str(n_tar - n_ms ) ) )
        if n_ms/n_tar < 0.99:
            print( 'Too many subbands failed, re-staging the following subbands:' )
            subbands = []
            for trf in glob.glob( os.path.join( caldir, '*tar' ) ):
                msname = '_'.join(trf.split('_')[:-1]) 
                if not os.path.exists( msname ):
                    subbands.append(os.path.basename(trf))
        srmfilename = obsid + '_srms.txt'
        srmpath='https://public.spider.surfsara.nl/project/lofarvlbi/srmlists/'
        response = requests.get(os.path.join(srmpath,srmfilename))
        data = response.text
        uris = data.rstrip('\n').split('\n')
        uris_to_stage = []
        for uri in uris:
            if os.path.basename(uri) in subbands:
                uris_to_stage.append(uri)
        stage_id = stager_access.stage(uris_to_stage)
        print('Re-staging failed subbands for {:s} (staging id {:s})'.format(obsid,str(stage_id)))
        update_status(obsid,'Staging',stage_id=stage_id)


if __name__=="__main__":

    fix_unpack()


