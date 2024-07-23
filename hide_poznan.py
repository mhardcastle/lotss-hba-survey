from __future__ import print_function
from surveys_db import SurveysDB
import requests
import os
import numpy as np

srmpath='https://public.spider.surfsara.nl/project/lofarvlbi/srmlists/'

with SurveysDB() as sdb:
    sdb.cur.execute('select * from lb_calibrators where status="Downloading" or status="Staged" or status="Not started"')
    results=sdb.cur.fetchall()
    for r in results:
        print(r['id'],r['status'])
        srmfilename = r['id'] + '_srms.txt'
        response = requests.get(os.path.join(srmpath,srmfilename))
        data = response.text
        uris = data.rstrip('\n').split('\n')
        if len(uris)==1 and uris[0]=='':
            print("Error, srmlist is empty!")
            update_status(id, 'SRMfile missing' )
        else:
            if 'psnc.pl' in uris[0]:
                print(r['id'],'in Poznan!')
                r['status']='Poznan'
                for k in ['username','clustername','nodename','location','start_date']:
                    r[k]=np.nan
                sdb.db_set('lb_calibrators',r)
                
                
