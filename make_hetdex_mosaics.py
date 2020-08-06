import glob
import os

os.chdir('/data/lofar/mjh/hetdex_v4/mosaics')
g=glob.glob('P*-mosaic.fits')

fields=[f.replace('-mosaic.fits','') for f in g]

mf=[]
for f in fields:
    if os.path.isdir('/data/lofar/DR2/mosaics/'+f):
        os.system('ln -s /data/lofar/DR2/mosaics/'+f+' /data/lofar/DR2/hetdex_v5/'+f)
    print 'Need to make a mosaic for',f
    mf.append(f)

for f in mf:
    print 'Doing',f
    hetdexdir='/data/lofar/DR2/hetdex_v5/'+f
    if os.path.isdir(hetdexdir):
        print 'Skipping, new directory exists'
        continue
    os.mkdir(hetdexdir)
    os.chdir(hetdexdir)
    os.system('mosaic_pointing.py --no-check --directories=/data/lofar/DR2/fields '+f)
    
