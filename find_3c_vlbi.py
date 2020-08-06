import os
from find_pos import Finder
from astropy.coordinates import SkyCoord,get_icrs_coordinates
from astropy.table import Table
import numpy as np
from auxcodes import sepn

def separation(ra1,dec1,ra2,dec2):
    return np.degrees(sepn(np.radians(ra1),np.radians(dec1),np.radians(ra2),np.radians(dec2)))

f=Finder()

os.chdir('/data/lofar/mjh/3crr')
lines=open('3crr.txt').readlines()
names=[l[:10].rstrip() for l in lines[2:]]
t=Table.read('/data/lofar/lbcs/lbcs_stats.sum',format='ascii')
t['col10'].name='ra'
t['col11'].name='dec'

for l in lines:
    bits=l.split()
    n=bits[0]
    c=get_icrs_coordinates(n)
    ra=float(c.ra.degree)
    dec=float(c.dec.degree)

    bf=f.find(ra,dec)
    if bf is None:
        continue
    field=bf['id']
    if bf['sep']<1.25:
        # Now look for an LBCS calibrator which is both (1) 1.25
        # degrees or less from the pointing centre and (2) 0.5 degrees
        # from the target.
        r=f.t[f.t['id']==field][0]
        fra=r['ra']
        fdec=r['decl']
        sep1=separation(ra,dec,t['ra'],t['dec'])
        sep2=separation(fra,fdec,t['ra'],t['dec'])
        filt=(sep1<0.5)
        filt&=(sep2<1.25)
        t['sep1']=sep1
        t['sep2']=sep2
        cals=t[filt]
        if len(cals)==0:
            continue
        print
        print n,field,bits[5],bits[7],bf['sep']
        print cals
        
    
