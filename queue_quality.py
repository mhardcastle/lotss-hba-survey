#!/usr/bin/env python

# Run the quality pipeline
# Herts-only code

from surveys_db import SurveysDB
import os
from subprocess import call,check_output
import sys

queued=[]
qlimit=32
queue=check_output('qstat -a',shell=True,universal_newlines=True).split('\n')
for l in queue:
    if 'qual-' in l:
        bits=l.split()
        field=bits[3][5:]
        print('Found',field,'already in queue')
        queued.append(field)

if len(queued)>=qlimit:
    print('Too many fields already in the queue, skipping')
    sys.exit(0)
        
with SurveysDB() as sdb:
    sdb.cur.execute('select * from fields left join quality on quality.id=fields.id where (status="Archived" or status="Verified") and archive_version>=4 and quality.catsources is NULL order by priority desc')
    results=sdb.cur.fetchall()

qcount=len(queued)
for r in results:
    
    id=r['id']
    dir='/data/lofar/DR3/fields/'+id
    if not os.path.isfile(dir+'/image_full_ampphase_di_m.NS.app.restored.fits'):
        print(id,'does not have the images!')
        continue
    if os.path.isfile(dir+'/image_full_ampphase_di_m.NS.cat.reg'):
        print(id,'has the quality catalogue')
    else:
        print(id,'does not have the quality catalogue')
    if id in queued:
        print('Not queueing it as it is already queued')
    else:
        if qcount<qlimit:
            os.system('qsub -l nodes=1:ppn=6,mem=20gb -N qual-%s -v WD=%s ~/pipeline-master/lotss-hba-survey/torque/quality.qsub' % (id,dir))
            qcount+=1
        else:
            print('Skipping as too many jobs already queued')
