#!/usr/bin/env python

# Get images for a field that has not been archived locally for some reason

from __future__ import print_function
from rclone import RClone
import os
import sys
from surveys_db import SurveysDB

def download_image(field,filename='images.tar',remote='archive'):
    archive='/data/lofar/DR3/fields/'
    wd=archive+field
    if not os.path.isdir(wd):
        os.mkdir(wd)
    os.chdir(wd)
    
    rc=RClone('maca_sksp_tape_DDF.conf',debug=True)
    rc.get_remote()
    remote=remote+'/'+field
    d=rc.execute_live(['-P','copy',rc.remote+'/'+remote+'/'+filename,wd])
    if d['err'] or d['code']!=0:
        raise RuntimeError('Download failed')
    os.system('tar xvf '+filename)
    os.system('rm '+filename)
    os.system('chmod og+r *')
    with SurveysDB() as sdb:
        sdb.create_quality(field)
    os.system('rm image_full_ampphase_di_m.NS.cat*')
    os.system('rm checksums.txt')

if __name__=='__main__':
    for name in sys.argv[1:]:
        download_image(name)
