#!/usr/bin/env python

from __future__ import print_function
import os
from surveys_db import SurveysDB
from auxcodes import warn,report
from rclone_upload import adler32_checksum

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
            files=['image_full_ampphase_di_m.NS.offset_cat.fits','image_full_ampphase_di_m.NS.offset_cat_radcat_compact.fits','Badfacets.txt']
            for f in files:
                if not os.path.isfile(f):
                    report('Copying '+f)
                    os.system('cp '+tim_dir+'/'+id+'/'+f+' .')
        if not error:
            checksum_lines=open(tim_dir+'/'+id+'/checksums.txt').readlines()
            cs={}
            for l in checksum_lines[1:]:
                bits=l.rstrip().split(',')
                cs[bits[0]]=bits[1]
            if not os.path.isfile('checksums.txt'):
                outfile=open('checksums.txt','w')
                for f in ['image_full_ampphase_di_m.NS.app.restored.fits','image_full_ampphase_di_m.NS.int.restored.fits']:
                    checksum=adler32_checksum(f)
                    if checksum!=cs[f]:
                        warn('Checksum does not match for '+f)
                        error=True
                    outfile.write(f+','+checksum+'\n')
                outfile.close()
        if not error:
            if not os.path.isfile('pslocal-facet_offsets.fits'):
                command='qsub -v FIELD=%s -N offset-%s /home/mjh/pipeline-offsetpointings/lotss-hba-survey/torque/find_offsets.qsub' % (id,id)
                print(command)
                os.system(command)
        if not error and (not os.path.isfile('image_full_ampphase_di_m.NS.app.restored_facetnoise.fits') or not os.path.isfile('image_full_low_m.app.restored_facetnoise.fits') or (r['decl']<14 and not os.path.isfile('image_full_ampphase_di_m.NS.app.restored_convolved_facetnoise.fits'))):
            command='qsub -v FIELD=%s -N noise-%s /home/mjh/pipeline-offsetpointings/lotss-hba-survey/torque/make_noisemap.qsub' % (id,id)
            print(command)
            os.system(command)
