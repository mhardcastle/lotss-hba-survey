import bdsf
from astropy.table import Table, vstack
from astropy.io import fits
import numpy as np
from astropy.coordinates import SkyCoord
import astropy.units as u
import os, glob
import argparse

def fix_imhead( fitsfile ):
    data, header = fits.getdata( fitsfile, header=True )
    if header['CRVAL1'] < 0:
        print( 'RA is negative, updating header.' )
        header['CRVAL1'] = header['CRVAL1'] + 360.
        fits.writeto( fitsfile, data, header, overwrite=True )
        print( 'done.' )
    else:
        print( 'RA is already positive, exiting.' )


def main( pointing, obsid, outdir='catalogue', catfile='pointing_catalogue.fits' ):
    ## pointing input file
    imcat_file = os.path.join(os.getenv('DATA_DIR'),pointing,'image_catalogue.csv')
    imcat = Table.read(imcat_file,format='csv')

    outcat = os.path.join(os.getenv('DATA_DIR'),pointing,catfile)

    ## output directory
    bdsf_outdir = os.path.join(os.getenv('DATA_DIR'),pointing,outdir)
    if not os.path.exists(bdsf_outdir):
        os.mkdir(bdsf_outdir)
    os.chdir(bdsf_outdir)

    ## get list of sources and loop over them
    sources = glob.glob(os.path.join(os.getenv('DATA_DIR'),pointing,obsid,'selfcal/ILT*'))

    ## check if they already exist in the catalogue
    if os.path.exists(outcat):
        pointing_cat = Table.read(outcat,format='fits')
    else:
        pointing_cat = Table()

    for source_file in sources:
        ## get lotss info on source
        source = os.path.basename(source_file)
        idx = np.where(imcat['Source_id'] == source)[0]
        lotss_info = imcat[idx]
        majax = lotss_info['Majax']*u.arcsec
        src_coords = SkyCoord( lotss_info['RA'], lotss_info['DEC'], unit='deg' )

        ## source detection
        imf = os.path.join( source_file, 'image_009-MFS-image-pb.fits' ) ## true flux
        appf = os.path.join( source_file, 'image_009-MFS-image.fits' ) ## apparent flux

        ## fix image header
        if os.path.exists(imf):        
            ## for now, until selfcal is re-run to save image-pb
            fix_imhead(imf)
        fix_imhead(appf)

        ## bdsf settings
        thresh_pix = 5
        thresh_isl = 4
        restfrq = 144000000.0

        if os.path.exists(imf):
            ## for now, until selfcal is re-run to save image-pb
            img = bdsf.process_image(imf, detection_image=appf, thresh_isl=thresh_isl, thresh_pix=thresh_pix, rms_box=(100,10), rms_map=True, mean_map='zero', ini_method='intensity', adaptive_rms_box=True, adaptive_thresh=150, rms_box_bright=(40,10), group_by_isl=False, group_tol=10.0, output_opts=True, output_all=False, atrous_do=True, atrous_jmax=4, flagging_opts=True, flag_maxsize_fwhm=0.5, advanced_opts=True, blank_limit=None, frequency=restfrq)
        else:
            img = bdsf.process_image(appf, thresh_isl=thresh_isl, thresh_pix=thresh_pix, rms_box=(100,10), rms_map=True, mean_map='zero', ini_method='intensity', adaptive_rms_box=True, adaptive_thresh=150, rms_box_bright=(40,10), group_by_isl=False, group_tol=10.0, output_opts=True, output_all=False, atrous_do=True, atrous_jmax=4, flagging_opts=True, flag_maxsize_fwhm=0.5, advanced_opts=True, blank_limit=None, frequency=restfrq)

        img.export_image(outfile='Default-'+source+'.rms.fits',img_format='fits', img_type='rms', clobber=True)
        img.export_image(outfile='Default-'+source+'.resid.fits',img_format='fits', img_type='gaus_resid', clobber=True)
        img.write_catalog(outfile='Default-'+source+'.srl.fits',format='fits', catalog_type='srl', clobber=True)
        img.write_catalog(outfile='Default-'+source+'.gaul.fits',format='fits', catalog_type='gaul', clobber=True)
        img.export_image(outfile='Default-'+source+'.model.fits',img_format='fits', img_type='gaus_model', clobber=True)
        img.export_image(outfile='Default-'+source+'.mask.fits',img_format='fits', img_type='island_mask', clobber=True)

        srl = Table.read('Default-'+source+'.srl.fits', format='fits')
        srl_coords = SkyCoord(srl['RA'], srl['DEC'], unit='deg' )
        seps = srl_coords.separation(src_coords)
        pointing_cat = vstack([pointing_cat,srl[np.where(seps <= majax)]])

    pointing_cat.write(outcat,format='fits')


if __name__ == "__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument( dest='ptgobsid', type=str )
    args = parser.parse_args()
    ptg = args.ptgobsid.split('/')[0]
    obsid = args.ptgobsid.split('/')[1]

    main( ptg, obsid )

    

