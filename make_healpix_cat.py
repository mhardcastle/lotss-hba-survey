import glob
from astropy.table import Table
from tqdm import tqdm
import os
from astropy_healpix import HEALPix
import astropy.units as u
from astropy.io import fits
from astropy.coordinates import SkyCoord

def run(s):
    print(s)
    os.system(s)

force=False
    
hp = HEALPix(nside=16)
print(hp.npix,'total healpix pixels on sky')
area=hp.pixel_area.value*3283
print('area of one healpix is',area,'sq. deg')

os.chdir('/beegfs/lofar/DR3/healpix_mosaics')

g=glob.glob('*')
g=[d for d in g if os.path.isdir(d)]
for cat in ['srl','gaul']:
    flist=open(f'{cat}-catlist.txt','w')
    for field in tqdm(g):
        if not os.path.isfile(field+'/mosaic.fits'):
            print(f'*** warning, {field}/mosaic.fits missing ***')
            continue
        with fits.open(field+'/mosaic.fits') as hdu:
            resolution=3600*hdu[0].header['BMAJ']
        infile=f'{field}/mosaic--final.{cat}.fits'
        if not os.path.isfile(infile):
            print(f'*** warning, {infile} missing ***')
            continue
        outname=infile.replace('--final','-filtered')
        if not os.path.isfile(outname) or force:
            pix=int(field)
            t=Table.read(infile)
            t['HEALPIX']=hp.lonlat_to_healpix(t['RA'].data*u.deg,t['DEC'].data*u.deg)
            t=t[t['HEALPIX']==pix]
            if len(t)==0:
                print(f'*** empty table {infile}, skipping ***')
                continue
            for k in t.colnames:
                if 'img_plane' in k or k.startswith('Resid') or k.endswith('_max') or k in ['Flag_beam','Isl_mean','Source_id','Isl_id']:
                    del(t[k])
            for k in ['E_RA','E_DEC','Maj','E_Maj','Min','E_Min','DC_Maj','E_DC_Maj','DC_Min','E_DC_Min']:
                t[k].convert_unit_to(u.arcsec)
            for k in ['Peak_flux','E_Peak_flux','Isl_rms']:
                t[k].convert_unit_to(u.mJy/u.beam)
            for k in ['Total_flux','E_Total_flux','Isl_Total_flux','E_Isl_Total_flux']:
                t[k].convert_unit_to(u.mJy)
            sc=SkyCoord(t['RA'],t['DEC'],frame='icrs')
            strings=sc.to_string(style='hmsdms',sep='',precision=2)
            ilt=[str('ILTJ'+s).replace(' ','')[:-1] for s in strings]
            t.add_column(ilt,name='Source_Name',index=0)
            t['Resolution']=resolution*u.arcsec
            t.write(outname,overwrite=True)
        flist.write(outname+'\n')
    flist.close()
    print('Now running STILTS')
    run(f'stilts tcat in=@{cat}-catlist.txt out={cat}.fits lazy=true ocmd="tablename LoTSS_DR3_{cat}; sort RA"')
