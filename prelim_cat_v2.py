#!/usr/bin/python

# construct a LOFAR 'DR2' catalogue by using the quality pipeline
# output as follows:

# * go through the fields in order of number of sources
# detected from highest to lowest (should be a proxy of field quality)

# * save WCS per field and find all the HEALPIX pixels occupied by all sources.
# Record in a dict mapping healpix pixels to field IDs

# * If there is overlap between the current field and an earlier field (defined by intersecting healpix pixels) use WCS for that field to exclude all sources from the current field that lie in the earlier field.

# * stack whatever's left.

from astropy_healpix import HEALPix
from astropy import units as u
from surveys_db import SurveysDB
from astropy.table import Table, vstack
from astropy.io import fits
from astropy.wcs import WCS
import numpy as np
import os

hp = HEALPix(nside=256)
print hp.npix
print hp.pixel_resolution

wcs={}
healpix={}
tlist=[]

with SurveysDB() as sdb:
    sdb.execute('select id, catsources from quality order by catsources desc')
    res=sdb.cur.fetchall()

os.chdir('/data/lofar/DR2/fields')
for r in res:
    field=r['id']
    print field
    hdu=fits.open(field+'/image_full_ampphase_di_m.NS_shift.int.facetRestored.fits')
    wcs[field]=WCS(hdu[0].header)
    t=Table.read(field+'/image_full_ampphase_di_m.NS.cat.fits')
    # cut a circle
    w=wcs[field]
    x,y,_,__=w.wcs_world2pix(t['RA'],t['DEC'],0,0,0)
    r=(x-9922)**2.0+(y-9922)**2.0
    t=t[r<4960**2.0]
    t['field']=field
    pixels=hp.lonlat_to_healpix(t['RA'], t['DEC'])
    t['pixel']=pixels
    plist=list(set(pixels))
    print len(plist),'unique healpix pixels'
    overlap_removed=[]
    print len(t),'sources originally'
    for pix in plist:
        if pix not in healpix:
            # this pixel does not overlap with anything
            healpix[pix]=[field]
        else:
            for overlap_field in healpix[pix]:
                if overlap_field in overlap_removed:
                    continue
                w=wcs[overlap_field]
                overlap_pos=w.wcs_world2pix(t['RA'],t['DEC'],0,0,0)
                x,y,_,__=overlap_pos
                r=(x-9922)**2.0+(y-9922)**2.0
                # not in the source circle of an overlap field
                t=t[r>4960**2.0]
                
                overlap_removed.append(overlap_field)
            healpix[pix].append(field)
    print len(t),'sources after overlap removal'
    tlist.append(t)

bigt=vstack(tlist)
bigt.write('bigtable_v2.fits')

    

    
