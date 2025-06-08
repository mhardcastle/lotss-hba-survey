#!/usr/bin/env python

# Clean up completed fields, copying some of the stuff we want to the DR3 fields directory

import sys
import os
import glob

for dir in sys.argv[1:]:
    print('Cleaning up',dir)
    files=['image_full_vlow_nocut_m.app.restored.fits','image_full_vlow_nocut_m.int.restored.fits','WSCLEAN_low-MFS-image.fits','WSCLEAN_low-MFS-image-int.fits']
    for f in files:
        if os.path.isfile(dir+'/'+f):
            print('Copying',f)
            os.system(f'cp {dir}/{f} /beegfs/lofar/DR3/fields/{dir}')
    g=glob.glob(dir+'/Dyn*tgz')
    if len(g):
        for f in g:
            print('Copying',f)
            os.system(f'cp {dir}/{f} /beegfs/lofar/DR3/fields/{dir}')
    os.system(f'rm -rf {dir}')

    
