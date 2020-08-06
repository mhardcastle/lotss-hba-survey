# check whether the QU cubes were updated after the run

from surveys_db import SurveysDB
import os
import datetime
import glob

with SurveysDB() as sdb:

    sdb.execute('select * from fields where status="Archived" and clustername="Herts" and location!=""')
    results=sdb.cur.fetchall()

updlist=[]
    
for r in results:
    print r['id'],r['end_date']
    g=glob.glob(r['location']+'/*.fz')
    if g<4:
        print '  *** not enough fz files ***'
    for f in g:
        created= os.stat(f).st_ctime
        cdt=datetime.datetime.fromtimestamp(created)
        if cdt>r['end_date'] and cdt>datetime.datetime(2018,9,26,0,0):
            print '  ',f,cdt
            updlist.append(r['id'])

updlist=set(updlist)
print updlist
