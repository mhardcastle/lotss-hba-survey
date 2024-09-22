#!/usr/bin/env python

from __future__ import print_function
import os
from surveys_db import SurveysDB
from find_mosaic_pointings import read_pointingfile, find_pointings_to_mosaic
import numpy as np

wd='/beegfs/lofar/DR3/fields/'
md='/beegfs/lofar/DR3/mosaics/'

with SurveysDB() as sdb:
    sdb.cur.execute('select * from fields where dr3>0 order by ra')
    results=sdb.cur.fetchall()

# 

rd={}
for r in results:
    field=r['id']
    output="%-12s %-12s %i %i " % (field,r['status'], r['dr3'],r['dr2'])
    if os.path.isdir(md+field):
        output+='! '
    else:
        output+='  '
    if r['status']!='Verified':
        output+='X'
    elif not os.path.isdir(wd+field):
        output+='X'
    else:
        output+='* '
        os.chdir(wd+field)
        output+='* ' if os.path.isfile('image_full_ampphase_di_m.NS.app.restored.fits') else 'X '
        output+='* ' if os.path.isfile('image_full_low_m.app.restored.fits') else 'X '
        output+='* ' if os.path.isfile('image_full_ampphase_di_m.NS.tessel.reg') else 'X '
        output+='* ' if os.path.isfile('pslocal-facet_offsets.fits') else 'X '
        output+='* ' if os.path.isfile('Badfacets.txt') else 'X '
        output+='* ' if os.path.isfile('image_full_ampphase_di_m.NS.app.restored_facetnoise.fits') else 'X '
        output+='* ' if os.path.isfile('image_full_low_m.app.restored_facetnoise.fits') else 'X '
    print(output)
    rd[field]=output

# Now check whether a field can be mosaiced
# This is the case if it is in the results list with all the files present and so are all its immediate neighbours
# Also add a check at some point that there is no current mosaic job running

print()
count=0
complete=0
pointingdict = read_pointingfile()
for r in results:
    field=r['id']
    if os.path.isdir(md+field) and os.path.isfile(md+field+'/mosaic-blanked.fits') and os.path.isfile(md+field+'/mosaic-blanked--final.srl.fits'):
        print('Mosaic directory for',field,'already exists!')
        complete+=1
        continue
    if 'X' in rd[field]:
        print('Cannot yet mosaic',field)
        continue
    print('Checking mosaic status for',field)
    mosaicpointings,mosseps = find_pointings_to_mosaic(pointingdict,field)
    maxsep=np.max(mosseps)
    print('There are %i pointings with max separation %f deg' % (len(mosaicpointings),maxsep))
    wanted_pointings=[]
    #ignored_pointings=[]
    pointing_failed=False
    for p in mosaicpointings:
        if p not in rd:
            continue # not DR3, will be ignored
        if 'X' in rd[p]:
            print('... pointing %s is not ready' % p)
            pointing_failed=True
        else:
            wanted_pointings.append(p)
    if not pointing_failed:
        print('Ready to mosaic with pointings ',wanted_pointings)
        print('qsub -v FIELD=%s -N mosaic-%s /home/mjh/pipeline-offsetpointings/lotss-hba-survey/torque/mosaic_dr3.qsub' % (field,field))
        count+=1

print('%i pointings are ready out of %i from DR3' % (count,len(results)))
print('%i pointings are complete' % complete)
