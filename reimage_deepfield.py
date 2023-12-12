#!/usr/bin/env python

# Reimage a deep field using a mask from the stacked individual
# images, which have all been processed using a model from the start
# field

# Steps are:
# Download the images and uv data for each field member plus the dicomodel for the start field
# Make a weighted stacked image
# Make a mask from the image
# ?? adjust the dataset weights ??
# Do an iteration of deconvolution on the whole field

from __future__ import print_function
from auxcodes import report,warn
from stack import stack
from surveys_db import SurveysDB
import sys
import os
from auxcodes import run as ddf_run
from rclone import RClone
import glob
from make_extended_mask import merge_mask
from reprocessing_utils import striparchivename
from fixsymlinks import fixsymlinks
from getcpus import getcpus
from get_cat import get_cat
#from offsets import do_offsets
#from pipeline import ddf_shift

def run(s):
    ddf_run(s,database=False)

def get_deepfield(fieldname):
    with SurveysDB(readonly=True) as sdb:
        r=sdb.db_get('deepfields',fieldname)
        if r is None:
            raise RuntimeError('Deep field does not exist!')
        startfield=r['start_field']
        sdb.execute('select * from fields where deepfield=%s',(fieldname,))
        result=sdb.cur.fetchall()
        if len(result)==0:
            raise RuntimeError('There are no fields with this deep field ID')
        fields=[]
        remotes=[]
        for r in result:
            if r['status']!='Verified':
                print('Field',r['id'],'has status',r['status'])
                raise RuntimeError('All fields must have verified status')
            fields.append(r['id'])
            if r['proprietary_date']:
                remotes.append('other')
            else:
                remotes.append('archive')
    return startfield,fields,remotes
    
def do_download_deepfield(fieldname):

    startfield,fields,remotes=get_deepfield(fieldname)
    print('About to download fields',fields)
    if not os.path.isdir(fieldname):
        os.mkdir(fieldname)
    os.chdir(fieldname)

    # Start the downloads
    report('Starting the download')
    rc=RClone('maca_sksp_tape_DDF.conf',debug=True)
    rc.get_remote()
    for f,r in zip(fields,remotes):
        imagename=f+'.app.restored.fits'
        if os.path.isfile(imagename):
            # use this as the indicator that the download's already happened
            warn('Image for field %s already exists, skipping download' % f)
            continue

        remote=r+'/'+f
        files=rc.get_files(remote)
        files=[fn for fn in files if 'uv' in fn or 'images' in fn]
        d=rc.multicopy(rc.remote+'/'+remote,files,os.getcwd())
        if d['err'] or d['code']!=0:
            raise RuntimeError('Download failed')
        for fn in files:
            if 'images' in fn:
                if f==startfield:
                    run('tar xvf '+fn+' image_full_ampphase_di_m.NS.mask01.fits image_full_ampphase_di_m.NS_shift.app.facetRestored.fits; mv image_full_ampphase_di_m.NS_shift.app.facetRestored.fits '+imagename)
                else:
                    run('tar xvf '+fn+' image_full_ampphase_di_m.NS_shift.app.facetRestored.fits; mv image_full_ampphase_di_m.NS_shift.app.facetRestored.fits '+imagename)
            elif 'misc' in fn:
                if f==startfield:
                    run('tar xvf '+fn+' DDS\\* image_dirin_SSD_m.npy.ClusterCat.npy image_full_ampphase_di_m.NS.DicoModel')
                else:
                    run('tar xvf '+fn+' DDS\\*')
            else:
                os.system('tar xvf '+fn) # since this may complain about hard links
    report('Tidying up')
    striparchivename()
    fixsymlinks('DDS3_full',delete_existing=True)
    fixsymlinks('DDS3_full_slow',stype='merged',verbose=True,delete_existing=True)

def do_download_deepfield_low(fieldname):

    startfield,fields,remotes=get_deepfield(fieldname)
    print('About to download low-res images for fields',fields)
    if not os.path.isdir(fieldname):
        os.mkdir(fieldname)
    os.chdir(fieldname)

    # Start the downloads
    report('Starting the download')
    rc=RClone('maca_sksp_tape_DDF.conf',debug=True)
    rc.get_remote()
    for f,r in zip(fields,remotes):
        imagename=f+'_low_m.app.restored.fits'
        if os.path.isfile(imagename):
            # use this as the indicator that the download's already happened
            warn('Image for field %s already exists, skipping download' % f)
            continue

        remote=r+'/'+f
        files=['images.tar']
        d=rc.multicopy(rc.remote+'/'+remote,files,os.getcwd())
        if d['err'] or d['code']!=0:
            raise RuntimeError('Download failed')
        for fn in files:
            if 'images' in fn:
                if f==startfield:
                    run('tar xvf '+fn+' image_full_low_m.app.restored.fits image_full_low_m.mask01.fits image_full_low_m.DicoModel; mv image_full_low_m.app.restored.fits '+imagename)
                else:
                    run('tar xvf '+fn+' image_full_low_m.app.restored.fits; mv image_full_low_m.app.restored.fits '+imagename)
    report('Tidying up')

        
def do_stack_deepfield(fieldname):
    if os.path.isdir(fieldname):
        os.chdir(fieldname)
    _,fields,_=get_deepfield(fieldname)

    # full-res stack and mask
    stackfile=fieldname+'.app.stack.fits'
    if os.path.isfile(stackfile):
        warn('Stack file %s exists, skipping the stack' % stackfile)
    else:
        report('Stacking')
        stack([f+'.app.restored.fits' for f in fields],outname=stackfile)
    maskfile=fieldname+'.mask-merged.fits'
    if os.path.isfile(maskfile):
        warn('Mask file exists, skipping mask')
    else:
        report('Making mask')
        run('MakeMask.py --Th=3 --Box=50,2 --RestoredIm='+stackfile)
        merge_mask(stackfile+'.mask.fits','image_full_ampphase_di_m.NS.mask01.fits',maskfile)

    # low-res stack and mask
    stackfile=fieldname+'_low_m.app.stack.fits'
    if os.path.isfile(stackfile):
        warn('Stack file %s exists, skipping the stack' % stackfile)
    else:
        report('Stacking low-res')
        stack([f+'_low_m.app.restored.fits' for f in fields],outname=stackfile)
    maskfile=fieldname+'_low.mask-merged.fits'
    if os.path.isfile(maskfile):
        warn('Mask file exists, skipping mask')
    else:
        report('Making mask')
        run('MakeMask.py --Th=3 --Box=50,2 --RestoredIm='+stackfile)
        merge_mask(stackfile+'.mask.fits','image_full_low_m.mask01.fits',maskfile)

def do_ddf_deepfield(fieldname,bmaj,bmin,bpa):
    # Beam in arcsec, bpa in the sense that 90 is north-south.
    if os.path.isdir(fieldname):
        os.chdir(fieldname)
    report('Running DDF')
    run('DDF.py --Misc-ConserveMemory=1 --Output-Name=image_fullband --Data-MS=big-mslist.txt --Deconv-PeakFactor 0.001000 --Data-ColName DATA --Parallel-NCPU=%i --Beam-CenterNorm=1 --Deconv-CycleFactor=0 --Deconv-MaxMinorIter=1000000 --Deconv-MaxMajorIter=1 --Deconv-Mode SSD --Beam-Model=LOFAR --Weight-Robust -0.500000 --Image-NPix=20000 --CF-wmax 50000 --CF-Nw 100 --Output-Also onNeds --Image-Cell 1.500000 --Facets-NFacets=11 --SSDClean-NEnlargeData 0 --Freq-NDegridBand 1 --Beam-NBand 1 --Facets-DiamMax 1.5 --Facets-DiamMin 0.1 --Deconv-RMSFactor=3.000000 --SSDClean-ConvFFTSwitch 10000 --Data-Sort 1 --Cache-Dir=. --Cache-DirWisdomFFTW=. --Debug-Pdb=never --Log-Memory 1 --GAClean-RMSFactorInitHMP 1.000000 --GAClean-MaxMinorIterInitHMP 10000.000000 --GAClean-AllowNegativeInitHMP True --DDESolutions-SolsDir=SOLSDIR --Cache-Weight=reset --Beam-LOFARBeamMode=A --Misc-IgnoreDeprecationMarking=1 --Beam-At=facet --Output-Mode=Clean --Output-RestoringBeam %f,%f,%f --Weight-ColName="IMAGING_WEIGHT" --Freq-NBand=2 --RIME-DecorrMode=FT --SSDClean-SSDSolvePars [S,Alpha] --SSDClean-BICFactor 0 --Mask-Auto=1 --Mask-SigTh=4.00 --Mask-External=%s.mask-merged.fits --DDESolutions-GlobalNorm=None --DDESolutions-DDModeGrid=AP --DDESolutions-DDModeDeGrid=AP --DDESolutions-DDSols=[DDS3_full_smoothed,DDS3_full_slow_merged] --Predict-InitDicoModel=image_full_ampphase_di_m.NS.DicoModel --Selection-UVRangeKm=[0.100000,1000.000000] --GAClean-MinSizeInit=10 --Beam-Smooth=1' % (getcpus(),bmaj,bmin,bpa,fieldname))
    report('Doing the shift')
    get_cat('pslocal',image='image_fullband.app.restored.fits')
    do_offsets({'mode':'normal','method':'pslocal','clusterfile':None,'cellsize':1.5,'fit':'mcmc'}, image_root='image_fullband')
    ddf_shift('image_fullband','facet-offset.txt',options={'restart':True,'cache_dir':'.','dryrun':False,'logging':None,'quiet':False})

def do_ddf_deepfield_low(fieldname):
    if os.path.isdir(fieldname):
        os.chdir(fieldname)
    report('Running DDF low')
    run('DDF.py --Misc-ConserveMemory=1 --Output-Name=image_fullband_low_m --Data-MS=big-mslist.txt --Deconv-PeakFactor 0.001000 --Data-ColName DATA --Parallel-NCPU=%i --Beam-CenterNorm=1 --Deconv-CycleFactor=0 --Deconv-MaxMinorIter=1000000 --Deconv-MaxMajorIter=1 --Deconv-Mode SSD --Beam-Model=LOFAR --Weight-Robust -0.250000 --Image-NPix=7000 --CF-wmax 50000 --CF-Nw 100 --Output-Also onNeds --Image-Cell 4.500000 --Facets-NFacets=11 --SSDClean-NEnlargeData 0 --Freq-NDegridBand 1 --Beam-NBand 1 --Facets-DiamMax 1.5 --Facets-DiamMin 0.1 --Deconv-RMSFactor=1.000000 --SSDClean-ConvFFTSwitch 10000 --Data-Sort 1 --Cache-Dir=. --Cache-DirWisdomFFTW=. --Debug-Pdb=never --Log-Memory 1 --GAClean-RMSFactorInitHMP 1.000000 --GAClean-MaxMinorIterInitHMP 10000.000000 --GAClean-AllowNegativeInitHMP True --DDESolutions-SolsDir=SOLSDIR --Cache-Weight=reset --Beam-LOFARBeamMode=A --Misc-IgnoreDeprecationMarking=1 --Beam-At=facet --Output-Mode=Clean --Output-RestoringBeam 20.000000 --Weight-ColName="IMAGING_WEIGHT" --Freq-NBand=2 --RIME-DecorrMode=FT --SSDClean-SSDSolvePars [S,Alpha] --SSDClean-BICFactor 0 --Mask-Auto=1 --Mask-SigTh=4.00 --Mask-External=%s_low.mask-merged.fits --DDESolutions-GlobalNorm=None --DDESolutions-DDModeGrid=AP --DDESolutions-DDModeDeGrid=AP --DDESolutions-DDSols=[DDS3_full_smoothed,DDS3_full_slow_merged] --Predict-InitDicoModel=image_full_low_m.DicoModel --Selection-UVRangeKm=[0.100000,25.750000] --GAClean-MinSizeInit=10 --Beam-Smooth=1' % (getcpus(),fieldname))
    
if __name__=='__main__':
    fieldname=sys.argv[1]
    #do_download_deepfield_low(fieldname)
    #do_stack_deepfield(fieldname)
    do_ddf_deepfield_low(fieldname)
    
# To add deep fields we need:
# Download all the images.tar and get the low-res images out
# Stack them
# Get the DicoModel and mask for the starting image
