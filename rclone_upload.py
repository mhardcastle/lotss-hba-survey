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

from upload import images, shiftimages

def make_tar(tarname,files,workdir,skip=True):
    outfile=tarname+'.tar'
    if os.path.isfile(workdir+'/_archive/'+outfile) and skip:
        print(outfile,'exists, skipping')
    else:
        command='cd %s; tar cvf _archive/%s.tar %s' % (workdir,tarname,' '.join(files))
        print('Running',command)
        retval=call(command,shell=True)
        if retval!=0:
            raise RuntimeError('tar command failed unexpectedly!')
    return [outfile]

class MyGlob(object):
    def __init__(self,workdir):
        self.workdir=workdir
    def glob(self,g):
        f=glob.glob(self.workdir+'/'+g)
        return [os.path.basename(file) for file in f]


def upload_field(name,basedir=None):
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

    print('Current field status is',idd['status'])

    update_status(name,'Creating tar',workdir=workdir)
    m=MyGlob(workdir)
    tars=[]
    tars+=make_tar('misc',['summary.txt','logs']+
                   m.glob('*-fit_state.pickle') +
                   m.glob('*.png') +
                   m.glob('*mslist*txt') +
                   m.glob('*crossmatch-results*') +
                   m.glob('*crossmatch-*.fits'),
                   workdir)
    tars+=make_tar('uv',m.glob('*.archive')+['SOLSDIR']+
                   m.glob('DDS*smoothed*.npz')+m.glob('DDS*full_slow*.npz'),workdir)
    imagelist=['image_dirin_SSD_m.npy.ClusterCat.npy','astromap.fits']+images('image_full_ampphase_di_m.NS',workdir)+images('image_full_low_m',workdir)
    if idd['do_spectral_restored']!=0:
        for i in range(3):
            imagelist+=shiftimages('image_full_ampphase_di_m.NS_Band%i' %i)
    tars+=make_tar('images',imagelist,workdir)

    if idd['do_polcubes']!=0:
        tars+=make_tar('stokes',m.glob('image_full_low_stokesV.dirty.*')+
                       m.glob('image_full_low_stokesV.SmoothNorm.fits')+
                       m.glob('image_full_low_QU.cube.*'),workdir)
        tars+=make_tar('stokes_vlow',m.glob('image_full_vlow_QU.cube.*'),workdir)
    if idd['do_dynspec']!=0:
        tars+=make_tar('dynspec',m.glob('DynSpecs*.tgz'),workdir)
    print('tars is',tars)
    update_status(name,'Created tar',workdir=workdir)

    rc=RClone('maca_sksp_tape_DDF.conf',debug=True)
    rc.get_remote()
    if other:
        remote='other/'+name
    else:
        remote='archive/'+name
    d=rc.execute_live(['-P','copy','_archive']+[rc.remote+'/'+remote])
    if d['err'] or d['code']!=0:
        raise RuntimeError('rclone upload failed!')
    update_status(name,'rcloned',workdir=workdir)

                      
    
if __name__=='__main__':
    import sys
    upload_field(sys.argv[1],sys.argv[2])
