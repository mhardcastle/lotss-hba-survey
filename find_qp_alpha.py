from astropy.table import Table
import glob
import os
import numpy as np
from surveys_db import SurveysDB

os.chdir('/data/lofar/DR2/fields')
g=glob.glob('*')

a1l=[]
a2l=[]

sdb=SurveysDB()

for d in g:
    infile=d+'/image_full_ampphase_di_m.NS.cat.fits_NVSS_match.fits'
    if os.path.isfile(infile):
        t=Table.read(infile)
        t=t[t['Total_flux']>0.01]
        l1=len(t)
        #alpha1=np.median(np.log(t['Total_flux']/t['NVSS_Total_flux'])/np.log(1400/144.0))
        alpha1=np.median(t['Total_flux']/t['NVSS_Total_flux'])
        t=t[t['Total_flux']>0.03]
        l2=len(t)
        #alpha2=np.median(np.log(t['Total_flux']/t['NVSS_Total_flux'])/np.log(1400/144.0))
        alpha2=np.median(t['Total_flux']/t['NVSS_Total_flux'])
        print d,alpha1,alpha2,l1,l2
        a1l.append(alpha1)
        a2l.append(alpha2)
        r=sdb.get_quality(d)
        r['nvss_scale']=alpha2
        sdb.set_quality(r)
        
print np.mean(a1l),np.mean(a2l)
sdb.close()
