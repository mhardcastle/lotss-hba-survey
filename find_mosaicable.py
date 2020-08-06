# find all fields that are mosaicable, i.e. have all 6 neighbouring fields, but are not currently mosaiced.

from __future__ import print_function
from find_mosaic_pointings import read_pointingfile, find_pointings_to_mosaic
import os
from surveys_db import SurveysDB

pointingdict = read_pointingfile()
with SurveysDB() as sdb:
    sdb.cur.execute('select id from fields where status="Archived" and dr1>0')
    r=sdb.cur.fetchall()

fields=[f['id'] for f in r]
for f in fields:
    print(f)
    if os.path.isfile('/data/lofar/DR2/mosaics/'+f+'/band2-mosaic-blanked.fits'):
        print('-- mosaic exists')
        if os.path.isfile('/data/lofar/DR2/mosaics/'+f+'/bandcat.cat.reg'):
            print('-- catalogue exists')
        else:
            print('-- catalogue needs to be made!')
            os.system('qsub -N mos_cat-%s -v FIELD=%s /home/mjh/pipeline-master/ddf-pipeline/torque/make_band_mos_cat.qsub' % (f,f))
    else:
        pointings,_ = find_pointings_to_mosaic(pointingdict,f)
        complete=False
        for p in pointings:
            if not os.path.isfile('/data/lofar/DR2/fields/'+p+'/image_full_ampphase_di_m.NS_Band2_shift.app.facetRestored.fits'):
                break
        else:
            complete=True
        if complete:
            print('-- to be mosaiced!')
            os.system('qsub -N bmosaic-%s -v FIELD=%s /home/mjh/pipeline-master/ddf-pipeline/torque/mosaic-band.qsub' % (f,f))
        else:
            print('-- neighbours not complete')
            
