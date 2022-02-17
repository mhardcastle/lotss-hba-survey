#!/usr/bin/env python
# Run pipeline download/unpack steps followed by the main job

from __future__ import absolute_import
from auxcodes import report,warn,die
from surveys_db import use_database,update_status
from download import download_dataset
from download_field import download_field
from unpack import unpack,unpack_db_update
from make_mslists import make_list,list_db_update
from check_structure import do_check_structure
from make_custom_config import make_custom_config
import sys
import os

rootdir= os.getcwd()
print(os.getcwd())
os.chdir(rootdir)

name=sys.argv[1]

if name[0]!='P' and name[0]!='L':
    die('This code should be used only with field or observation names',database=False)

do_field=(name[0]=='P')

try:
    os.mkdir(name)
except OSError:
    warn('Working directory already exists')
    pass
print(os.getcwd())
os.chdir(name)
report('Downloading data')

if do_field:
    success=download_field(name,basedir=rootdir)
#else:
#    success=download_dataset('https://lofar-webdav.grid.sara.nl','/SKSP/'+name+'/')

if not success:
    die('Download failed, see earlier errors',database=False)

#print(os.getcwd())
#os.chdir(name)
    
report('Unpacking data')
unpack()
if do_field:
    unpack_db_update()

workdir = os.getcwd()
print(workdir)
    
report('Deleting tar files')
os.system('rm *.tar.gz')
os.system('rm '+workdir+'/*.tar')

report('Checking structure')
try:
    averaged,dysco=do_check_structure(workdir=workdir)
except RuntimeError:
    if do_field:
        update_status('Check failed',workdir=workdir)
    raise

report('Making ms lists')
success=make_list()
if do_field:
    list_db_update(success,workdir=workdir)
if not success:
    die('make_list could not construct the MS list',database=False)

report('Creating custom config file from template')
make_custom_config(name,workdir,do_field,averaged)

if success:
    report('Submit job')
    os.system('pipeline.py tier1-config.cfg')
    #if do_field():
    #    update_status(name,'Queued')

else:
    die('make_list could not construct the MS list',database=False)


if os.path.exists('image_full_ampphase_di_m.NS_shift.app.facetRestored.fits'):
	upload_field(name,rootdir)
else:
	report('DOESNT SEEM TO HAAVE FINISHED')
