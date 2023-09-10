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

def run(s):
    ddf_run(s,database=False)

def do_reimage_deepfield(fieldname):
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
                
    print('About to reimage using fields',fields)
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
                os.system('tar xvf '+fn) # since this will complain about hard links
        
    

if __name__=='__main__':
    fieldname=sys.argv[1]
    do_reimage_deepfield(fieldname)
