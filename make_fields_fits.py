from surveys_db import SurveysDB
import matplotlib.pyplot as plt
from astropy.table import Table
import numpy as np

# pack up the quality results into an astropy table for ease of plotting columns

with SurveysDB() as sdb:
    sdb.cur.execute('select id,ra,decl,gal_l,gal_b,status,priority,weave_priority from fields order by id')
    results=sdb.cur.fetchall()

td={}
for key in results[0]:
    print key
    vl=[]
    for r in results:
        vl.append(r[key])
    vl=[np.nan if f is None else f for f in vl]
    td[key]=vl

t=Table(td)
t['ra'].name='RA'
t['decl'].name='DEC'
t=t['id','RA','DEC','gal_l','gal_b','status','priority','weave_priority']
t.write('fields.fits',overwrite=True)
