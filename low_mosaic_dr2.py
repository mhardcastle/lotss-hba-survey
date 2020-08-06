import numpy as np
import os

lines=open(os.environ['DDF_DIR']+'/ddf-pipeline/misc/DR2-pointings.txt').readlines()
wd='/data/lofar/DR2/low_mosaics'

for l in lines:
    bits=l.split()
    ra=float(bits[1])
    if ra<139 or ra>250:
        continue
    name=bits[0]
    print name
    dd=wd+'/'+name
    if not(os.path.isfile(dd+'/low-mosaic.fits')):
        if not(os.path.isdir(dd)):
            os.mkdir(dd)
        os.chdir(dd)
        os.system('mosaic_pointing.py --directories /data/lofar/DR2/fields --no-highres --do-lowres --do_scaling --no-check '+name)
    
