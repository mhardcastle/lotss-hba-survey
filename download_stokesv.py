from __future__ import print_function
from run_extraction_pipeline import do_rclone_download
from surveys_db import SurveysDB
import glob
import os
import sys

with SurveysDB() as sdb:
    sdb.cur.execute('select id,status,clustername,nodename,location,start_date,end_date from fields where status="Verified" and dr2=0')
    results=sdb.cur.fetchall()

try:
    target_dir=sys.argv[1]
except:
    print('Please specify a directory to download to')
    sys.exit(1)
if not os.path.isdir(target_dir):
    os.mkdir(target_dir)
    
for r in results:
    id=r['id']
    dir='/data/lofar/DR2/fields/'+id
    g=glob.glob(dir+'/*stokesV*fits')
    if len(g)==0:
        # create target dir
        target=target_dir+'/'+id
        if not os.path.isdir(target):
            os.mkdir(target)
        g=glob.glob(target+'/*stokesV*fits')
        if len(g)==0:
            print('We need to download data for',id)
            do_rclone_download(id,target,tarfiles=['stokes_small.tar'])
            #os.system('cd %s; rm *QU*.fz' % target)
            
