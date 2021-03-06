import os
from find_pos import Finder
from astropy.coordinates import SkyCoord,get_icrs_coordinates
from make_subim import extract_and_save
from astropy.table import Table
from astropy.io import fits
import numpy as np

def extract_list(t,resolve=False,check_exists=True):
    f=Finder()
    ''' t is an astropy table with columns name and optionally ra,dec '''
    for r in t:
        n=r['name']
        filename=n+'.fits'
        if check_exists and os.path.isfile(filename):
            continue
        if resolve:
            c=get_icrs_coordinates(n)
            ra=float(c.ra.degree)
            dec=float(c.dec.degree)
        else:
            ra=r['ra']
            dec=r['dec']
        bf=f.find(ra,dec)
        if bf is None:
            print('%s not found' % n)
            continue
        field=bf['id']
        success=False
        if bf['sep']<1.95 and os.path.isdir('/data/lofar/DR2/mosaics/'+field):
            # extract from mosaics dir
            print('Extracting %s from mosaic %s' % (n, field))
            extract_and_save('/data/lofar/DR2/mosaics/'+field+'/mosaic-blanked.fits',ra,dec,0.5,outname=filename)
            fieldhdu=fits.open('/data/lofar/DR2/fields/'+field+'/image_full_ampphase_di_m.NS_shift.int.facetRestored.fits')
            date=fieldhdu[0].header['DATE-OBS']
            fieldhdu.close()
            hdu=fits.open(filename)
            maxy,maxx=hdu[0].data.shape
            success=~np.isnan(hdu[0].data[maxy//2,maxx//2])
            if not success:
                print('Image is centrally blanked!')
            else:
                hdu[0].header['DATE-OBS']=date
                hdu[0].header['OBJECT']=field+'_mosaic'
                hdu.writeto(filename,overwrite=True)
                
        if not success:
            scale=5.9124/bf['nvss_scale']
            print('Extracting %s from field %s with scale factor %.3f' % (n,field,scale))
            extract_and_save('/data/lofar/DR2/fields/'+field+'/image_full_ampphase_di_m.NS_shift.int.facetRestored.fits',ra,dec,0.5,outname=n+'.fits',scale=scale)
            

if __name__=='__main__':
    os.chdir('/data/lofar/mjh/3crr')
    lines=open('3crr.txt').readlines()
    names=[l[:10].rstrip() for l in lines[2:]]

    t=Table([names],names=['name'])
    extract_list(t,resolve=True,check_exists=False)
