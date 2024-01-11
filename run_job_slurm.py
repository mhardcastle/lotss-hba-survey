#!/usr/bin/env python
#Run job

from __future__ import print_function
from surveys_db import update_status, SurveysDB
from auxcodes import report,warn,die
from make_custom_config import choose_batch_file
import sys
import os
import glob

def do_run_job(name,basedir,batchfile=None,do_field=True,prefix='ddfp',dysco=False):
    config=''
    workdir=basedir+'/'+name
    g=glob.glob(workdir+'/tier1*.cfg')
    if len(g)>0:
        print('Local config file exists, using that')
        config=',CONFIG='+g[0]
    if batchfile is None:
        batchfile=choose_batch_file(name,workdir,do_field)
    report('Submit job')

    os.system('export FIELD='+name+'; sbatch --export=FIELD --job-name=ddf+'+name+' /home/azimuth/pipeline.sh')
    if do_field:
        update_status(name,'Queued',workdir=workdir)
        
if __name__=='__main__':
    name=sys.argv[1]
    try:
        batchfile=sys.argv[2]
    except:
        batchfile=None

    do_run_job(name,os.env['DDF_BASEDIR'],batchfile)
