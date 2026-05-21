#!/usr/bin/env python
# Call with the ILT name or other object name plus a region size in degrees

from __future__ import print_function
from __future__ import absolute_import
from find_pos import find_pos
import sys
import os
from astropy.coordinates import SkyCoord,get_icrs_coordinates
from make_subim import extract_and_save
import astropy.units as u
from astropy_healpix import HEALPix

def parse_position(argv):
    # take a calling argument list and return name, position and size
    objname=argv[1]
    try:
        imsize=float(argv[2])
    except:
        imsize=0.5

    if 'ILTJ' in objname:
        s=objname[4:]
        coord=s[0:2]+':'+s[2:4]+':'+s[4:9]+' '+s[9:12]+':'+s[12:14]+':'+s[14:]
        sc = SkyCoord(coord,unit=(u.hourangle,u.deg))
        ra=sc.ra.value
        dec=sc.dec.value
        print('Parsed coordinates to ra=%f, dec=%f' % (ra,dec))
    elif objname.startswith('pos'):
        ra=float(argv[3])
        dec=float(argv[4])
        try:
            bits=objname.split('_')
            objname=bits[1]
        except:
            objname='pos-%f-%f' % (ra,dec)
    else:
        c=get_icrs_coordinates(objname)
        ra=float(c.ra.degree)
        dec=float(c.dec.degree)
        print('Coordinate lookup gives ra=%f, dec=%f' % (ra,dec))
    return objname,ra,dec,imsize

if __name__=='__main__':
    print('** This code now looks at the mosaics directory! **')
    objname,ra,dec,imsize=parse_position(sys.argv)
    hp=HEALPix(nside=16)
    pixel=hp.lonlat_to_healpix(ra*u.deg,dec*u.deg)
    print('Looking at healpix pixel',pixel)
    wd='/beegfs/lofar/DR3/healpix_mosaics/'+str(pixel)
    if not os.path.isdir(wd):
        raise RuntimeError('Directory does not exist')
    print('Extracting FULL total intensity cutout')
    extract_and_save(wd+'/mosaic.fits',ra,dec,imsize,outname=objname+'_I.fits')
    print('Extracting LOW total intensity cutout')
    extract_and_save(wd+'/low-mosaic.fits',ra,dec,imsize,outname=objname+'_I_low.fits')
        
