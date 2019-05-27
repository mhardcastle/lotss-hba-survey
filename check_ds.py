#!/usr/bin/python

import sys
import os
from surveys_db import SurveysDB
import numpy as np
from make_dr2_release import download_file
from time import sleep
import glob

with SurveysDB(readonly=True) as sdb:
    sdb.cur.execute('select fields.id as field,fields.ra,fields.decl,observations.id as obsid,observations.date,fields.end_date,fields.clustername as clustername from observations right join fields on fields.id=observations.field where fields.status="Archived" and observations.status="DI_Processed"')
    results=sdb.cur.fetchall()
for r in results:
    path='/data/lofar/DR2/fields/'+r['field']+'/DynSpecs_L'+str(r['obsid'])+'.tgz'
    if not os.path.isfile(path):
        print 'Missing',path,'(',r['clustername'],')',r['field']
        g=glob.glob('/data/lofar/DR2/fields/'+r['field']+'/DynSpecs_L*.tgz')
        if len(g)>0:
            print g
        '''
        os.chdir('/data/lofar/DR2/fields/'+r['field'])
        download_file(r['field'],'DynSpecs_L'+str(r['obsid'])+'.tgz')
        sleep(10)
        '''
