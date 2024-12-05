#!/usr/bin/env python

# like make_i_subim.py, but do on the fly mosaicing where possible,
# including large areas

from __future__ import print_function
from __future__ import absolute_import
from find_pos import Finder
from astropy.io import fits
from astropy.coordinates import SkyCoord,get_icrs_coordinates
from mosaic import make_mosaic
from make_i_subim import parse_position
import sys
from auxcodes import dotdict
from surveys_db import SurveysDB # for scale factor
from astropy.wcs import WCS
import numpy as np
import os

def otf_mosaic(objname,ra,dec,imsize,beamcut=0.1,do_low=False):
    # Do the on-the-fly mosaicing. ra, dec, imsize in degrees.
    # Return True on success or False on failure (no images)
    f=Finder()
    t=f.find(ra,dec,offset=np.sqrt(3.5**2+imsize**2),verbose=True,return_t=True)
    print(t)
    if len(t)==0: return False

    # open the central image to get BMAJ, BMIN
    for n in range(len(t)):
        cfield=t[n]
        images='/beegfs/lofar/DR3/fields'
        try:
            if do_low:
                hdu=fits.open(images+'/'+cfield['id']+'/image_full_low_m.app.restored.fits')
            else:
                hdu=fits.open(images+'/'+cfield['id']+'/image_full_ampphase_di_m.NS_shift.app.facetRestored.fits')
            break
        except:
            pass
    else:
        return False # no image we can open
    
    # create a header that we can pass to the mosaic command
    psize=hdu[0].header['CDELT2'] # pixel size in degrees
    header=fits.Header()
    header['NAXIS']=2
    header['BITPIX']=hdu[0].header['BITPIX']
    header['NAXIS1']=int(round(imsize/psize))
    header['NAXIS2']=int(round(imsize/psize))
    header['WCSAXES']=2
    header['CRPIX1']=header['NAXIS1']/2
    header['CRPIX2']=header['NAXIS2']/2
    header['CDELT1']=-psize
    header['CDELT2']=psize
    header['CTYPE1']='RA---SIN'
    header['CTYPE2']='DEC--SIN'
    header['CUNIT1']='deg'
    header['CUNIT2']='deg'
    header['CRVAL1']=ra
    header['CRVAL2']=dec
    header['LONPOLE']=180.0
    header['LATPOLE']=dec
    header['EQUINOX']=2000.0
    header['RADESYS']='ICRS'
    header['BMAJ']=hdu[0].header['BMAJ']
    header['BMIN']=hdu[0].header['BMIN']

    # later on we'll need to check BMAJ and BMIN, or maybe the mosaic
    # script itself needs to do that.

    header['BPA']=hdu[0].header['BPA']
    header['RESTFRQ']=hdu[0].header['RESTFRQ']
    header['TELESCOP']='LOFAR'
    header['OBSERVER']='LoTSS'
    header['OBJECT']=objname
    #header['DATE_OBS']=hdu[0].header['DATE_OBS']
    header['ORIGIN']='Mosaic'
    header['BTYPE']='Intensity'
    header['BUNIT']='Jy/beam'

    #print(header)

    with SurveysDB() as sdb:
        mosaicdirs=[]
        scales=[]
        for r in t:
            if r['status']!='Archived' and r['status']!='Verified':
                continue
            if r['proprietary_date']:
                continue
            p=r['id']
            images='/beegfs/lofar/DR3/fields'
            mosaicdirs.append(images+'/'+p)
            try:
                qualitydict = sdb.get_quality(p)
                scale= 1.0/(qualitydict['nvss_scale']/5.9124)
                scales.append(scale)
            except TypeError:
                missingpointing = True
                print('No scaling factor for ',p)
                scales.append(1.0)

    
    mos_args=dotdict({'save':False, 'load':False,'exact':False,'use_shifted':True,'find_noise':True,'beamcut':beamcut,'header':header,'directories':mosaicdirs,'scale':scales,'do_lowres':do_low})
    print(mos_args)
    make_mosaic(mos_args)
    os.system('mv mosaic.fits %s-mosaic-low.fits' % objname)
    os.system('mv mosaic-weights.fits %s-mosaic-low-weights.fits' % objname)
    return True
    
if __name__=='__main__':
    if len(sys.argv)<2:
        raise RuntimeError('You must give an object name or position')
    objname,ra,dec,imsize=parse_position(sys.argv)
    otf_mosaic(objname,ra,dec,imsize,do_low=True)
    
