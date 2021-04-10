# mosaic the vlow stokes I images

from __future__ import print_function
from surveys_db import SurveysDB
import os

def run(c):
    print(c)
    result=os.system(c)
    if result!=0:
        raise RuntimeError('Failed to run '+c)

with SurveysDB(readonly=True) as sdb:
    sdb.cur.execute('select id from fields where dr2')
    results=sdb.cur.fetchall()

os.chdir('/beegfs/car/mjh/vlow_temp')
for r in results:
    field=r['id']
    if os.path.isfile('/beegfs/car/mjh/vlow_mosaic/'+field+'-mosaic.fits'):
        print(field,'done, skipping')
        continue
    command='mosaic_pointing.py --directories /data/lofar/DR2/fields --do-vlow --no-check --no-bdsf --do_scaling '+field
    run(command)
    for f in ['vlow-mosaic-blanked.fits','vlow-mosaic.fits','vlow-mosaic-weights.fits']:
        run('mv '+f+' /beegfs/car/mjh/vlow_mosaic/'+f.replace('vlow',field))
    run('rm *')
    
