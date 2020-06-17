from surveys_db import SurveysDB
from astropy.table import Table
import numpy as np

t=Table.read('/home/mjh/pipeline-master/ddf-pipeline/misc/DR2-pointings.txt',format='ascii')
colnames=['Field','RA','Dec']
for i,c in enumerate(t.colnames):
    t[c].name=colnames[i]

fields=['0h','8h','13h']

t0h=t[(t['RA']<50) | (t['RA']>300)]
t8h=t[(t['RA']<140) & (t['RA']>90)]
t13h=t[(t['RA']>140) & (t['RA']<250)]
ft=[t0h,t8h,t13h]

for field,t in zip(fields,ft):
    print field,len(t)

with SurveysDB() as sdb:
    sdb.cur.execute('select fields.id,fields.ra,fields.decl,fields.status,quality.* from fields left join quality on fields.id=quality.id order by fields.id')
    results=sdb.cur.fetchall()

td={}
for r in results:
    td[r['id']]=r

for field,t in zip(fields,ft):
    print field,len(t)
    complete=0
    incomplete=0
    incomplete_fields=[]
    rmsmean=[]
    for r in t:
        name=r['Field']
        if td[name]['rms'] is None:
            incomplete+=1
            incomplete_fields.append(name)
        else:
            rmsmean.append(4.2*td[name]['rms']/td[name]['nvss_scale'])
            complete+=1

    print complete,incomplete,np.median(rmsmean)
    for f in incomplete_fields:
        print f,':',td[f]['status'],'//',
    print
    
