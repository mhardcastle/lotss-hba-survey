#!/usr/bin/env python
# Run pipeline download/unpack steps followed by the main job

from __future__ import print_function
from __future__ import absolute_import
from builtins import zip
from auxcodes import report,warn,die
from surveys_db import SurveysDB,update_status
from download import download_dataset
from download_field import download_field
from run_job import do_run_job
from unpack import unpack
from make_mslists import make_list,list_db_update
from make_custom_config import make_custom_config,choose_qsub_file
from check_structure import do_check_structure
import numpy as np
import sys
import os
import glob
from rclone import RClone

def do_run_pipeline(name,basedir,qsubfile=None,do_field=True):
    '''
    set do_field False for the now obsolete behaviour of downloading
    and imaging a particular observation -- this probably doesn't work any more
    with do_field True the database must be present
    '''
    workdir=basedir+'/'+name
    try:
        os.mkdir(workdir)
    except OSError:
        warn('Working directory already exists')

    if do_field:
        with SurveysDB() as sdb: idd=sdb.get_field(name)
        
    report('Downloading data')
    if do_field:
        if idd['location']==workdir and idd['status']!='Not started':
            warn('Refusing to re-download a dataset that exists locally')
            success=True
        else:
            success=download_field(name,basedir=basedir)
    else:
        success=download_dataset('https://lofar-webdav.grid.sara.nl','/SKSP/'+name+'/',basedir=basedir)

    if not success:
        die('Download failed, see earlier errors',database=False)

    report('Unpacking data')
    if do_field and idd['location']==workdir and (idd['status']=='Unpacked' or idd['status']=='Ready'):
        warn('Looks like the data have been unpacked already, skipping this step')
    else:
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
            update_status(name,'Check failed',workdir=workdir)
        raise

    report('Making ms lists')
    if do_field and idd['status']=='Ready' and idd['location']==workdir:
        warn('Looks like the MS lists have already been made, skipping')
    else:
        success=make_list(workdir=workdir)
        if do_field:
            list_db_update(success,workdir=workdir)
        if not success:
            die('make_list could not construct the MS list',database=False)

    kwargs={}
    if do_field:
        if idd['deepfield']:
             with SurveysDB() as sdb: df=sdb.db_get('deepfields',idd['deepfield'])
             startfield=df['start_field']
        else:
            df=None
            startfield=None
        if df:
            if startfield!=name:
                # This is a non-starting field in this deep field.  We
                # check to see if the template image files already
                # exist in the directory where they're stored (saves a
                # download). If not we download and unpack.  We need
                # the images and uv_misc.tar files (OK to assume that
                # split-uv was used -- maybe!) We need to check whether
                # the field is proprietary or not since if it is it
                # will be stored in 'other' not 'archive'.
                templatefiles=['image_dirin_SSD_m.npy.ClusterCat.npy','image_full_ampphase_di_m.NS.app.restored.fits','image_full_ampphase_di_m.NS.mask01.fits','image_full_ampphase_di_m.NS.DicoModel']
                with SurveysDB() as sdb: sf=sdb.get_field(startfield)
                if sf['status']!='Verified':
                    raise RuntimeError('Trying to run a non-starting field when starting field is not Verified')
                if sf['proprietary_date']:
                    remote='other'
                else:
                    remote='archive'
                
                if 'DDF_DOWNLOAD_CACHE' in os.environ:
                    target=os.environ['DDF_DOWNLOAD_CACHE']+'/'+startfield
                else:
                    target=workdir+'/'+template
                i=0
                if os.path.isdir(target):
                    for f in templatefiles:
                        if os.path.isfile(target+'/'+f): i+=1
                if i==len(templatefiles):
                    warn('Download directory and files already exist -- skipping')
                else:
                    report('Downloading template files')
                    update_status(name,'Downloading template',workdir=workdir)
                    if not os.path.isdir(target): os.mkdir(target)
                    rc=RClone('maca_sksp_tape_DDF.conf',debug=True)
                    rc.get_remote()
                    remote=remote+'/'+startfield
                    d=rc.multicopy(rc.remote+'/'+remote,['uv_misc.tar','images.tar'],target)
                    if d['err'] or d['code']!=0:
                        update_status(name,'Template download failed',workdir=workdir)
                        raise RuntimeError('Template download failed')
                    
                    os.system('cd %s; tar xvf uv_misc.tar; tar xvf images.tar; rm *.tar' % target)
                if 'template' not in target: # cached, presumably --
                                             # copy the files we need
                                             # to the template
                                             # directory
                    report('Copying template files')
                    tdir=workdir+'/template'
                    if not os.path.isdir(tdir):
                        os.mkdir(tdir)
                    os.system('cd '+target+'; cp '+' '.join(templatefiles)+' '+tdir)
                # At this point the files required should be in the template directory of workdir.
                kwargs={'tdir':tdir}
                update_status(name,'Ready',workdir=workdir)
            
    report('Creating custom config file from template')
    make_custom_config(name,workdir,do_field,averaged,**kwargs)

    if qsubfile is None:
        report('Choosing qsub file')
        qsubfile=choose_qsub_file(name,workdir,do_field)
    
    # now run the job
    do_run_job(name,basedir=basedir,qsubfile=qsubfile,do_field=do_field,dysco=dysco)


if __name__=='__main__':
            
    try:
        qsubfile=sys.argv[2]
    except:
        qsubfile=None
    do_run_pipeline(sys.argv[1],'/beegfs/car/mjh',qsubfile=qsubfile)
