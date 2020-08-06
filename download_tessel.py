#!/usr/bin/python

# Get the missing tessel files

from surveys_db import SurveysDB
import os
from subprocess import call
from time import sleep

if os.environ['DDF_PIPELINE_CLUSTER']!='paracluster':
    target=os.environ['DDF_PIPELINE_LEIDENUSER']+'@ssh.strw.leidenuniv.nl:'
else:
    target=''

with SurveysDB() as sdb:
    sdb.cur.execute('select * from fields where archive_version=4 and clustername="Herts" and status="Archived"')
    results=sdb.cur.fetchall()

for r in results:
    id=r['id']
    dir='/data/lofar/DR2/fields/'+id
    if os.path.isfile(dir+'/image_full_ampphase_di_m.NS.tessel.reg'):
        print id,'has the tessel file'
    else:
        print id,'does not have the tessel file'
        os.chdir(dir)
        s='rsync -avz  --timeout=20 --progress '+target+'/disks/paradata/shimwell/LoTSS-DR2/archive/'+id+'/image_full_ampphase_di_m.NS.tessel.reg .'
        while True:
            print 'Running command:',s
            retval=call(s,shell=True)
            if retval==0:
                break
            print 'Non-zero return value',retval
            if retval!=30:
                raise RuntimeError('rsync failed unexpectedly')
            sleep(10)


