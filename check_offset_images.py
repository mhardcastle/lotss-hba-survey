#!/usr/bin/env python

from __future__ import print_function
import os
from surveys_db import SurveysDB
from auxcodes import warn,report
from rclone_upload import adler32_checksum
from astropy.table import Table

# Check whether all the files are present for the offset finding to be run
# We need:
# unshifted .app.restored.fits and .int.restored.fits (actually not required for the offset finding but we might as well check for them anyway)
# a tessel.reg file
# Tim's source catalogues

# If the source catalogues are not present then they can be copied from Tim's directory

# Use the database as source of truth

with SurveysDB() as sdb:
    sdb.cur.execute('select * from fields where dr3>0 and status="Verified"')
    results=sdb.cur.fetchall()

dir='/beegfs/lofar/DR3/fields'
tim_dir='/beegfs/car/shimwell/LoTSS-DR3-pointings/pointing-details'
dryrun=False

for r in results:
    error=False
    id=r['id']
    print('----',id,'----')
    wd=dir+'/'+id
    if not os.path.isdir(wd):
        warn('Directory '+wd+' does not exist')
        error=True
    else:
        os.chdir(dir+'/'+id)
    
        files=['image_full_ampphase_di_m.NS.app.restored.fits','image_full_ampphase_di_m.NS.int.restored.fits','image_full_ampphase_di_m.NS.tessel.reg']
        for f in files:
            if not os.path.isfile(f):
                warn('File '+f+' does not exist')
                error=True
        if not os.path.isdir(tim_dir+'/'+id):
            warn("Tim's directory does not exist")
            error=True
        else:
            files=['image_full_ampphase_di_m.NS.offset_cat.fits','image_full_ampphase_di_m.NS.offset_cat_radcat_compact.fits','Badfacets.txt','pslocal-facet_offsets.fits']
            for f in files:
                if not os.path.isfile(f):
                    report('Copying '+f)
                    if not dryrun: os.system('cp '+tim_dir+'/'+id+'/'+f+' .')
        if not error and os.path.isfile(tim_dir+'/'+id+'/checksums.txt'):
            checksum_lines=open(tim_dir+'/'+id+'/checksums.txt').readlines()
            cs={}
            for l in checksum_lines[1:]:
                bits=l.rstrip().split(',')
                cs[bits[0]]=bits[1]
            if not os.path.isfile('checksums.txt'):
                output=[]
                for f in ['image_full_ampphase_di_m.NS.app.restored.fits','image_full_ampphase_di_m.NS.int.restored.fits']:
                    checksum=adler32_checksum(f)
                    if checksum!=cs[f]:
                        warn('Checksum does not match for '+f)
                        error=True
                    output.append(f+','+checksum+'\n')
                if not error:
                    with open('checksums.txt','w') as outfile:
                        outfile.writelines(output)
        if not error:
            offsets='pslocal-facet_offsets.fits'
            offsetfile=os.path.isfile(offsets)
            if offsetfile:
                t=Table.read(offsets)
                if 'RA_peak' not in t.colnames:
                    print('Offsets file is old version')
                    os.unlink(offsets)
                    offsetfile=False
            if not offsetfile:
                command='qsub -v FIELD=%s -N offset-%s /home/mjh/pipeline-master/lotss-hba-survey/torque/find_offsets.qsub' % (id,id)
                print(command)
                if not dryrun: os.system(command)
        if not error and (not os.path.isfile('image_full_ampphase_di_m.NS.app.restored_facetnoise.fits') or not os.path.isfile('image_full_low_m.app.restored_facetnoise.fits') or (r['decl']<14 and not os.path.isfile('image_full_ampphase_di_m.NS.app.restored_convolved_facetnoise.fits'))):
            command='qsub -v FIELD=%s -N noise-%s /home/mjh/pipeline-master/lotss-hba-survey/torque/make_noisemap.qsub' % (id,id)
            print(command)
            if not dryrun: os.system(command)
