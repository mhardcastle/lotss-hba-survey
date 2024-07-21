import glob
import os
from surveys_db import SurveysDB

os.chdir('/data/lofar/DR3/fields')
fields=glob.glob('*')

with SurveysDB() as sdb:

    for f in fields:
        if not os.path.isdir(f):
            continue
        if os.path.isdir('/data/lofar/DR2/fields/'+f):
            print(f)
            sdb.cur.execute('update quality set catsources=NULL where id=%s',(f,))
