# check whether all verified images are stored locally: download if not

from __future__ import print_function
from surveys_db import SurveysDB
from rclone import RClone
import glob
import os

workdir='/data/lofar/DR2/fields'

with SurveysDB() as sdb:
    sdb.cur.execute('select id from fields where status="Verified" order by id')
    results=sdb.cur.fetchall()

for r in results:
    field=r['id']
    directory=workdir+'/'+field
    if not os.path.isdir(directory):
        print(field,'-- directory does not exist')
        os.path.mkdir(directory)
    if not len(glob.glob(directory+'/*')):
        print(field,'-- directory has no content')
        rc=RClone('maca_sksp_tape_DDF.conf',debug=True)
        rc.get_remote()
        d=rc.execute_live(['-P','copy',rc.remote+'/archive/'+field+'/images.tar',directory])
        if d['err'] or d['code']!=0:
            print('Rclone failed')
        else:
            print('Rclone OK, untarring!')
            os.system('cd %s; tar xvf images.tar; rm images.tar' % directory)
