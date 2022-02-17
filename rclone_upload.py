#!/usr/bin/env python

# Upload a completed field using rclone
# Use database to work out what should be uploaded
# Proceed by first creating, then uploading, then removing tar files

from __future__ import print_function

from rclone import RClone
from surveys_db import SurveysDB,update_status

import sys
import os
import glob
from subprocess import call
from auxcodes import report, warn, die
from upload import images, shiftimages
from fixsymlinks import fixsymlinks

from astropy.io import fits
from zlib import adler32
#import timeout_decorator

import argparse

readme={'README.txt':'This file',
        'summary.txt':'Summary of ddf-pipeline run',
        'logs':'Logfile directory',
        'fits_headers':'FITS headers of image files',
        'mslist.txt':'Original MS list',
        'big-mslist.txt':'Full MS list',
        'pslocal-fit_state.pickle':'Full offsets fitting structure',
        'SKO-pslocal.png':'Offset plot for the field',
        'crossmatch-1.fits':'Bootstrap catalogue iteration 1',
        'crossmatch-2.fits':'Bootstrap catalogue iteration 2',
        'crossmatch-results-1.npy':'Bootstrap results iteration 1',
        'crossmatch-results-2.npy':'Bootstrap results iteration 2',
        'astromap.fits':'Astrometry accuracy map',
        'image_full_ampphase_di_m.NS.app.restored.fits':'Full-resolution final image in apparent flux',
        'image_full_ampphase_di_m.NS.int.restored.fits':'Full-resolution final primary-beam corrected image',
        'image_full_ampphase_di_m.NS_shift.app.facetRestored.fits':'Full-resolution final image in apparent flux with astrometric corrections applied',
        'image_full_ampphase_di_m.NS_shift.int.facetRestored.fits':'Full-resolution final primary-beam corrected image with astrometric corrections applied',
        'image_full_ampphase_di_m.NS.int.model.fits':'Full-resolution model image',
        'image_full_ampphase_di_m.NS.int.residual.fits':'Full-resolution residual image',
        'image_full_ampphase_di_m.NS.mask01.fits':'Full-resolution final mask',
        'image_full_ampphase_di_m.NS.DicoModel':'Full-resolution final DicoModel',
        'image_full_ampphase_di_m.NS.tessel.reg':'Full-resolution facet region file',
        'image_full_low_m.app.restored.fits':'20-arcsec resolution final image in apparent flux',
        'image_full_low_m.int.restored.fits':'20-arcsec resolution final primary-beam corrected image',
        'image_full_low_m.int.model.fits':'20-arcsec resolution model image',
        'image_full_low_m.int.residual.fits':'20-arcsec resolution residual image',
        'image_full_low_m.mask01.fits':'20-arcsec resolution final mask',
        'image_full_low_m.DicoModel':'20-arcsec resolution final DicoModel',
        'image_full_low_m.tessel.reg':'20-arcsec resolution facet region file',
        'image_full_ampphase_di_m.NS_Band0_shift.app.facetRestored.fits':'Full-resolution Band 0 image in apparent flux',
        'image_full_ampphase_di_m.NS_Band0_shift.int.facetRestored.fits':'Full-resolution Band 0 primary-beam corrected image',
        'image_full_ampphase_di_m.NS_Band1_shift.app.facetRestored.fits':'Full-resolution Band 1 image in apparent flux',
        'image_full_ampphase_di_m.NS_Band1_shift.int.facetRestored.fits':'Full-resolution Band 1 primary-beam corrected image',
        'image_full_ampphase_di_m.NS_Band2_shift.app.facetRestored.fits':'Full-resolution Band 2 image in apparent flux',
        'image_full_ampphase_di_m.NS_Band2_shift.int.facetRestored.fits':'Full-resolution Band 2 primary-beam corrected image',
        'image_full_low_QU.cube.dirty.fits.fz':'Compressed undeconvolved 20-arcsec resolution QU cube in apparent flux',
        'image_full_low_QU.cube.dirty.corr.fits.fz':'Compressed undeconvolved 20-arcsec resolution QU cube with primary beam correction',
        'image_full_low_stokesV.dirty.fits':'Undeconvolved 20-arcsec resolution Stokes V image in apparent flux',
        'image_full_low_stokesV.dirty.corr.fits':'Undeconvolved 20-arcsec resolution Stokes V image with primary beam correction',
        'image_full_low_stokesV.SmoothNorm.fits':'Undeconvolved 20-arcsec resolution Stokes V image with smooth primary beam correction',
        'image_full_stokesV.dirty.fits':'Undeconvolved full-resolution Stokes V image in apparent flux',
        'image_full_stokesV.dirty.corr.fits':'Undeconvolved full-resolution Stokes V image with primary beam correction',
        'image_full_stokesV.SmoothNorm.fits':'Undeconvolved full-resolution Stokes V image with smooth primary beam correction',
        'image_full_vlow_QU.cube.dirty.fits.fz':'Compressed undeconvolved 60-arcsec resolution QU cube in apparent flux',
        'image_full_vlow_QU.cube.dirty.corr.fits.fz':'Compressed undeconvolved 60-arcsec resolution QU cube with primary beam correction'}


def adler32_checksum(filename):

    BLOCKSIZE=256*1024*1024
    asum = 1
    with open(filename,'rb') as f:
        while True:
            data = f.read(BLOCKSIZE)
            if not data:
                break
            asum = adler32(data, asum)
            if asum < 0:
                asum += 2**32
    checksum_local = hex(asum)[2:10].zfill(8).lower()

    return checksum_local

#@timeout_decorator.timeout(5,use_signals=False)
def extract_header(fitsfile):
    hdrs=[]
    with fits.open(fitsfile) as fhand:
        for hdu in fhand:
            hdrs.append(hdu.header)
    return hdrs
        

def dump_headers(workdir,files,verbose=False):
    # code from Yan Grange adapted to work in general directory
    try:
        os.mkdir(workdir+"/fits_headers")
    except OSError:
        pass       # We don't care if the dir already exists

    for fitsfile in files:
        if fitsfile.endswith('.fz') or fitsfile.endswith('.fits'):
            if verbose: print(fitsfile)
            #try:
            hdrs = extract_header(workdir+'/'+fitsfile)
            #except timeout_decorator.timeout_decorator.TimeoutError:
            #    print('File header %s read timed out! Is it corrupt?' % fitsfile)
            #    hdrs=[]
            for ctr, hdr in enumerate(hdrs):
                hdr.totextfile(workdir+"/fits_headers/"+fitsfile+"."+str(ctr)+".hdr", overwrite=True)

class Tarrer(object):
    ''' Instrumented tar file creator which can keep a record of FITS files used and make readme files on the fly '''
    def __init__(self,workdir):
        self.workdir=workdir
        self.files=[]
        self.tars=[]

    def make_readme(self,tarname,files):

        with open(self.workdir+'/README.txt','w') as outfile:
            outfile.write('Contents for '+tarname+'.tar\n\n')
            contents=[]
            ml=0
            for f in sorted(files):
                d='???'
                if f in readme:
                    d=readme[f]
                else:
                    for k,v in readme.items():
                        if f.endswith(k):
                            d=readme[k]
                contents.append([f,d])
                if len(f)>ml:
                    ml=len(f)
            for f,d in contents:
                outfile.write(("%-"+str(ml+1)+"s: %s\n") % (f,d))

    def remove_readme(self):
        os.unlink(self.workdir+'/README.txt')
                
    def make_tar(self,tarname,files,skip=True,readme=False):
        report('Creating '+tarname)
        workdir=self.workdir
        outfile=tarname+'.tar'
        if readme:
            files.append('README.txt')
            self.make_readme(tarname,files)
        
        if os.path.isfile(workdir+'/_archive/'+outfile) and skip:
            warn(outfile+' exists, skipping')
        else:
            command='cd %s; tar cf _archive/%s.tar %s' % (workdir,tarname,' '.join(files))
            warn('Running '+command)
            retval=call(command,shell=True)
            if retval!=0:
                die('tar command failed unexpectedly!',database=False)
        self.files+=files
        self.tars.append(outfile)
        if readme:
            self.remove_readme()

class MyGlob(object):
    '''trivial glob wrapper that is initialized with a working directory'''
    def __init__(self,workdir):
        self.workdir=workdir
    def glob(self,g):
        f=glob.glob(self.workdir+'/'+g)
        return [os.path.basename(file) for file in f]


def upload_field(name,basedir=None,split_uv=False):
    '''Upload the field 'name' stored in the specified directory
        'basedir'.  Do not rely on changing working directory to
        basedir so that this can be run in parallel with other code.'''

    workdir=basedir+'/'+name
    try:
        os.mkdir(workdir+'/_archive')
    except OSError:
        pass
    
    with SurveysDB(readonly=True) as sdb:
        idd=sdb.get_field(name)

    other=False
    if not (idd['lotss_field']>0) or idd['proprietary_date'] is not None:
        other=True

    warn('Current field status is '+idd['status'])

    report('Fixing symlinks...')
    fixsymlinks('DDS3_full',verbose=False)
    fixsymlinks('DDS3_full_slow',stype='merged',verbose=False)
    fixsymlinks('DDS2_full',verbose=False)
    report('Creating tar files')
    update_status(name,'Creating tars',workdir=workdir)
    m=MyGlob(workdir)
    t=Tarrer(workdir)

    if split_uv:
        # break the uv tar file into obsids and a misc section
        t.make_tar('uv_misc',['image_dirin_SSD_m.npy.ClusterCat.npy','image_full_ampphase_di_m.NS.DicoModel','image_full_ampphase_di_m.NS.tessel.reg']+m.glob('DDS*smoothed*.npz')+m.glob('DDS*full_slow*.npz'))
        mslist=m.glob('*.archive')
        obsids=set([os.path.basename(ms).split('_')[0] for ms in mslist])
        for obsid in obsids:
            t.make_tar('uv_'+obsid,m.glob(obsid+'_*.archive')+m.glob('SOLSDIR/'+obsid+'_*'))
    else:
        t.make_tar('uv',m.glob('*.archive')+['image_dirin_SSD_m.npy.ClusterCat.npy','image_full_ampphase_di_m.NS.DicoModel','image_full_ampphase_di_m.NS.tessel.reg','SOLSDIR']+m.glob('DDS*smoothed*.npz')+m.glob('DDS*full_slow*.npz'))
        
    imagelist=['astromap.fits']+images('image_full_ampphase_di_m.NS',workdir)+images('image_full_low_m',workdir)
    imagelist+=shiftimages('image_full_ampphase_di_m.NS')
    if idd['do_spectral_restored']!=0:
        for i in range(3):
            imagelist+=shiftimages('image_full_ampphase_di_m.NS_Band%i' %i)
    t.make_tar('images',imagelist,readme=True)

    if idd['do_polcubes']!=0:
        t.make_tar('stokes_large',
                       m.glob('image_full_low_QU.cube.*.fz'),readme=True)
        t.make_tar('stokes_small',m.glob('image_full_*_stokesV.dirty.*')+
                       m.glob('image_full_*_stokesV.SmoothNorm.fits')+m.glob('image_full_vlow_QU.cube.*.fz'),readme=True)
    if idd['do_dynspec']!=0:
        t.make_tar('dynspec',m.glob('DynSpecs*.tgz'))

    # now we can get FITS headers from files we've added
    report('Make FITS header directory')
    dump_headers(workdir,t.files,verbose=False)
        
    t.make_tar('misc',['summary.txt','logs','fits_headers','mslist.txt','big-mslist.txt']+
                   m.glob('*-fit_state.pickle') +
                   m.glob('*.png') +
                   m.glob('*crossmatch-results*') +
                   m.glob('*crossmatch-*.fits'),readme=True)

    update_status(name,'Created tar',workdir=workdir)
    report('Uploading')
    rc=RClone('maca_sksp_tape_DDF.conf',debug=True)
    rc.get_remote()
    if other:
        remote='other/'+name
    else:
        remote='archive/'+name
    d=rc.execute_live(['-P','copy',workdir+'/_archive']+[rc.remote+'/'+remote])
    if d['err'] or d['code']!=0:
        update_status(name,'rclone failed',workdir=workdir)
        die('rclone upload failed!',database=False)
    update_status(name,'rcloned',workdir=workdir)

    report('Validate checksums')

    checksums={}
    for tarfile in t.tars:
        print('Checking',tarfile)
        remote_checksum=rc.get_checksum(remote+'/'+tarfile)
        print('Computing local checksum, please wait...')
        local_checksum=adler32_checksum(workdir+'/_archive/'+tarfile)
        if remote_checksum!=local_checksum:
            update_status(name,'checksum failed',workdir=workdir)
            die('Checksum failed! '+remote_checksum+' '+local_checksum,database=False)
        else:
            print('Checksum OK!')
            checksums[tarfile]=local_checksum
            with SurveysDB() as sdb:
                sdb.execute('insert into checksums values ( %s, %s, %s)',(name,tarfile, local_checksum))

    update_status(name,'Verified',workdir=workdir)

    report('Tidying up')
    for tarfile in t.tars:
        os.unlink(workdir+'/_archive/'+tarfile)
    os.rmdir(workdir+'/_archive')

    report('Upload completed successfully!')
    return checksums
    
if __name__=='__main__':
    parser = argparse.ArgumentParser(description='Rclone upload')
    parser.add_argument('fields', metavar='N', type=str, nargs='+',
                    help='Fields to upload')
    parser.add_argument('--split_uv', action='store_true',help='Split the UV tar files')
    parser.add_argument('--working_dir', type=str, default='.', help='Working directory')
    args = parser.parse_args()
    
    for f in args.fields:
        result=upload_field(f,args.working_dir,split_uv=args.split_uv)
        print('Field',f,'checksum dictionary was',result)
    
