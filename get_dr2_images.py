#!/usr/bin/env python

# Get images for a field that has not been archived locally for some reason

from __future__ import print_function
from rclone import RClone
import os
import sys
from reprocessing_utils import do_sdr_and_rclone_download
import urllib3

def download_image(field,filename='images.tar',remote='archive'):
    archive='/data/lofar/DR3/fields/'
    wd=archive+field
    if not os.path.isdir(wd):
        os.mkdir(wd)
    os.chdir(wd)
    
    do_sdr_and_rclone_download(field,wd,Mode="ImageOnly",verbose=True)

if __name__=='__main__':
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    for name in sys.argv[1:]:
        download_image(name)
