#!/usr/bin/env python

# Make a cube out of the three band images and run pybdsf

import sys
import os
from auxcodes import warn
import numpy as np
from astropy.io import fits
import bdsf

field=sys.argv[1]
os.chdir('/data/lofar/DR2/mosaics/'+field)

hdus=[]
for i in range(3):
    h=fits.open('band%i-mosaic.fits' %i)
    hdus.append(h)
    y,x=h[0].data.shape

newdata=np.zeros((3,y,x),dtype=np.float32)
for i,h in enumerate(hdus):
    newdata[i,:,:]=h[0].data

ohdu=hdus[0]

    
outfile='band-cube.fits'
if os.path.isfile(outfile):
    warn('Output file exists, skipping cube generation')
else:
    ohdu[0].data=newdata
    ohdu[0].header['NAXIS3']=3
    ohdu[0].header['CTYPE3']='FREQ'
    ohdu[0].header['CUNIT3']='Hz'
    ohdu[0].header['CRPIX3']=1
    ohdu[0].header['CRVAL3']=128e6
    ohdu[0].header['CDELT3']=16.0e6
    ohdu[0].header['WCSAXES']=3
    ohdu.writeto(outfile,overwrite=True)

os.system('chmod u+w .')
os.system('chmod u+w mosaic-blanked_pybdsm')
catprefix='bandcat'
img = bdsf.process_image(outfile, thresh_isl=4.0, thresh_pix=5.0, rms_box=(150,15), rms_map=True, mean_map='zero', ini_method='intensity', adaptive_rms_box=True, adaptive_thresh=150, rms_box_bright=(60,15), group_by_isl=False, group_tol=10.0, output_opts=True, output_all=True, atrous_do=True, atrous_jmax=4, flagging_opts=True, flag_maxsize_fwhm=0.5,advanced_opts=True, blank_limit=None, spectralindex_do=True,specind_maxchan=1, flagchan_rms=False,flagchan_snr=False,frequency=144e6,collapse_mode='file',collapse_file='mosaic-blanked.fits',incl_chan=True)
img.write_catalog(outfile=catprefix +'.cat.fits',catalog_type='srl',format='fits',correct_proj='True',clobber=True,incl_chan=True)
img.write_catalog(outfile=catprefix +'.cat.srl',catalog_type='srl',format='ascii',correct_proj='True',clobber=True,incl_chan=True)
img.export_image(outfile=catprefix +'.rms.fits',img_type='rms',img_format='fits',clobber=True)
img.export_image(outfile=catprefix +'.resid.fits',img_type='gaus_resid',img_format='fits',clobber=True)
img.export_image(outfile=catprefix +'.pybdsmmask.fits',img_type='island_mask',img_format='fits',clobber=True)
img.write_catalog(outfile=catprefix +'.cat.reg',catalog_type='srl',format='ds9',correct_proj='True',clobber=True)
