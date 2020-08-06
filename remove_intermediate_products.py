#!/usr/bin/python

# Tidy up some products on the way to a full deletion

import os
from surveys_db import SurveysDB
import glob

with SurveysDB() as sdb:
    sdb.cur.execute('select * from fields where status="Archived" and clustername="Herts"')
    results=sdb.cur.fetchall()

for r in results:
    print r['id'],r['location']
    os.chdir(r['location'])
    if len(glob.glob('*.fz'))==4:
        os.system('rm -r *_QU_Cube*.fits')

             
