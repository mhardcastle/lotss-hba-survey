#!/usr/bin/python

from __future__ import print_function
from surveys_db import SurveysDB
import subprocess
import os

from plot_db_status import qstat

q=qstat()
with SurveysDB(readonly=True) as sdb:
    sdb.cur.execute('select * from fields where clustername="Herts" and (status="Running" or status="Queued") order by status,id')
    fields=sdb.cur.fetchall()

for f in fields:
    id=f['id']
    if id not in q:
        print(id,'has status',f['status'],'but is not in queue')
        if f['location'] is not None and os.path.isdir(f['location']):
            os.system('ls -tlr '+f['location']+' | tail -1')
    else:
        print(id,'has status',f['status'],'and queue status',q[id])
