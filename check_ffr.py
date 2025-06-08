#!/usr/bin/env python

import sys
import os
from surveys_db import SurveysDB
import glob
import subprocess

def qstat():
    # adapted from plot_db
    try:
        results=subprocess.check_output(['qstat', '-a'],text=True).split('\n')
    except TypeError:
        results=subprocess.check_output(['qstat', '-a']).split('\n')
    except OSError:
        results=[]
    jobs={}
    for l in results:
        bits=l.split()
        if len(bits)>10:
            jobname=bits[3]
            status=bits[9]
            if 'ffr-' in jobname:
                jobs[jobname[4:]]=status
    return jobs

wd='/beegfs/car/mjh/ffr'
os.chdir(wd)
g=sorted(glob.glob('*'))

q=qstat()
with SurveysDB(readonly=True) as sdb:
    for f in g:
        sdb.cur.execute('select * from full_field_reprocessing where status!="Verified" and id=%s',(f,))
        results=sdb.cur.fetchall()
        sdb.cur.execute('select * from full_field_reprocessing where status="Not started" and id=%s',(f,))
        nsresults=sdb.cur.fetchall()
        sdb.cur.execute('select * from full_field_reprocessing where status="Staging" and id=%s',(f,))
        stresults=sdb.cur.fetchall()
        if len(results)==0:
            print(f,'all verified, can delete')
            if len(sys.argv)>1 and sys.argv[1]=='delete':
                os.system('cleanup_ffr.py '+f)
        else:
            print(f,'not complete (%i), ' % len(results),end='')
            if f in q:
                print('in queue, status is',q[f])
            else:
                if len(nsresults)==len(results):
                    print('all not started (download fail)')
                else:
                    if len(results)==len(stresults):
                        print('all staging')
                    else:
                        print('not in queue')
            
