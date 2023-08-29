#!/usr/bin/env python
# Run pipeline download/unpack steps followed by the main job

from __future__ import print_function
from __future__ import absolute_import
from builtins import zip
from auxcodes import report,warn,die
from surveys_db import SurveysDB,update_status
from download import download_dataset
from download_field import download_field
from run_job_slurm import do_run_job
from unpack import unpack
from make_mslists import make_list,list_db_update
from make_custom_config import make_custom_config,choose_batch_file
from check_structure import do_check_structure
import numpy as np
import sys
import os
import glob

def do_run_pipeline(name,basedir,batchfile=None,do_field=True):

    if not do_field:
        raise RuntimeError('do_run_pipeline with do_field False is not supported')
    
    workdir=basedir+'/'+name
    try:
        os.mkdir(workdir)
    except OSError:
        warn('Working directory already exists')

    report('Downloading data')
    success=download_field(name,basedir=basedir)

    if not success:
        die('Download failed, see earlier errors',database=False)

    report('Unpacking data')
    try:
        unpack(workdir=workdir)
    except RuntimeError:
        if do_field:
            update_status(name,'Unpack failed',workdir=workdir)
        raise
    if do_field:
        update_status(name,'Unpacked',workdir=workdir)

    report('Deleting tar files')
    os.system('rm '+workdir+'/*.tar.gz')
    os.system('rm '+workdir+'/*.tar')

    report('Checking structure')
    try:
        averaged,dysco=do_check_structure(workdir=workdir)
    except RuntimeError:
        if do_field:
            update_status('Check failed',workdir=workdir)
        raise
    
    report('Making ms lists')
    success=make_list(workdir=workdir)
    if do_field:
        list_db_update(success,workdir=workdir)
    if not success:
        die('make_list could not construct the MS list',database=False)
        
    report('Creating custom config file from template')
    make_custom_config(name,workdir,do_field,averaged)

    if batchfile is None:
        report('Choosing batch file')
        batchfile=choose_batch_file(name,workdir,do_field)
    
    # now run the job
    do_run_job(name,basedir=basedir,batchfile=batchfile,do_field=do_field,dysco=dysco)


if __name__=='__main__':
            
    try:
        batchfile=sys.argv[2]
    except:
        batchfile=None
    do_run_pipeline(sys.argv[1],os.environ['DDF_BASEDIR'],batchfile=batchfile)