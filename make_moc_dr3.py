from surveys_db import SurveysDB
from mocpy import MOC
from astropy import units as u
from astropy_healpix import HEALPix
import numpy as np
from tqdm import tqdm

hp = HEALPix(nside=1024,order='nested') 

with SurveysDB(readonly=True) as sdb:
    sdb.cur.execute('select ra,decl from fields where dr3>0')
    result=sdb.cur.fetchall()

ra=[]
dec=[]
for r in result:
    ra.append(r['ra'])
    dec.append(r['decl'])

plist=[]
clist=list(zip(ra,dec))
for r,d in tqdm(clist):
    pixels=hp.cone_search_lonlat(r*u.deg,d*u.deg,1.85*u.deg)
    plist=plist+list(pixels)

p=set(plist)
pn=np.array(list(p),dtype=np.uint64)
depth=np.array([10]*len(pn),dtype=np.uint8)
moc=MOC.from_healpix_cells(pn,depth,max_depth=max(depth)+1)
moc.write('/home/mjh/dr3-moc.moc',overwrite=True)
