#!/usr/bin/python

from __future__ import print_function
from surveys_db import SurveysDB
import subprocess
import os
import sys

flag_failed=(len(sys.argv)>1 and sys.argv[1]=='flag_failed')
rerun=(len(sys.argv)>1 and sys.argv[1]=='rerun')

from plot_db_status import qstat

q=qstat()
reruns=[]
with SurveysDB(readonly=not flag_failed) as sdb:
    sdb.cur.execute('select * from fields where clustername="Herts" and (status="Running" or status="Queued") order by status,id')
    fields=sdb.cur.fetchall()

    for f in fields:
        id=f['id']
        found=id in q
        if not found:
            for k in q:
                if id.startswith(k):
                    found=True
                    key=k
                    break
        else:
            key=id
        if not found:
            print(id,'has status',f['status'],'but is not in queue')
            if flag_failed:
                f['status']='Failed (running)'
                sdb.set_field(f)
            if rerun:
                reruns.append(id)
        else:
            print(id,'has status',f['status'],'and queue status',q[key])
        if f['location'] is not None and os.path.isdir(f['location']):
            os.system('ls -tlr '+f['location']+' | tail -1')

if rerun:
    for id in reruns:
        os.system('run_job.py '+id)
        
