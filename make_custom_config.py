#!/usr/bin/env python

from __future__ import print_function
import os
import sys
from surveys_db import SurveysDB
import numpy as np
from add_galcoords import add_galcoords

def choose_qsub_file(name,workdir,do_field):
    if do_field:
        with SurveysDB() as sdb:
            idd=sdb.get_field(name)
        #if np.abs(idd['gal_b'])<=10:
        #    return '/home/mjh/pipeline-master/lotss-hba-survey/torque/pipeline_MW.qsub'
        #else:
        return '/home/mjh/pipeline-master/lotss-hba-survey/torque/pipeline.qsub'
    else:
        return None # leave it to something else to choose
        
def make_custom_config(name,workdir,do_field,averaged=False):
    if do_field:
        with SurveysDB() as sdb:
            idd=sdb.get_field(name)
            if idd['gal_b'] is None:
                print('Missing Galactic co-ordinates, adding them')
                add_galcoords(sdb,[idd])
                idd=sdb.get_field(name)

        no_wenss=((idd['decl']<32) | (idd['decl']>72))
        no_tgss=(idd['no_tgss']==1)
        if idd['lotss_field']>0:
            lotss_field=True
            do_polcubes=True
            do_dynspec=True
            do_stokesv=True
            do_spectral_restored=True
        else:
            lotss_field=False
            do_polcubes=(idd['do_polcubes']>0)
            do_dynspec=(idd['do_dynspec']>0)
            do_stokesv=(idd['do_stokesv']>0)
            do_spectral_restored=(idd['do_spectral_restored']>0)
    else:
        # assume LOTSS defaults
        no_wenss=False
        no_tgss=False
        lotss_field=True

    if np.abs(idd['gal_b'])<10.0:
        template=os.environ['DDF_DIR']+'/ddf-pipeline/examples/tier1-jul2018-MW.cfg'
    else:
        if no_wenss:
            if idd['decl']<10.0:
                template=os.environ['DDF_DIR']+'/ddf-pipeline/examples/tier1-jul2018-lowdec.cfg'
            else:
                template=os.environ['DDF_DIR']+'/ddf-pipeline/examples/tier1-jul2018-NVSS.cfg'
        else:
            template=os.environ['DDF_DIR']+'/ddf-pipeline/examples/tier1-jul2018.cfg'

    lines=open(template).readlines()
    outfile=open(workdir+'/tier1-config.cfg','w')
    for l in lines:
        if 'colname' in l and averaged:
            outfile.write('colname=DATA\n')
        elif '[control]' in l and no_tgss:
            outfile.write(l+'redo_DI=True\n')
        elif 'do_dynspec' in l and not do_dynspec:
            outfile.write('do_dynspec=False\n')
        elif 'spectral_restored' in l and not do_spectral_restored:
            outfile.write('spectral_restored=False\n')
        elif 'polcubes' in l and 'compress' not in l and not do_polcubes:
            outfile.write('polcubes=False\n')
        elif 'stokesv' in l and not do_stokesv:
            outfile.write('stokesv=False\n')
        else:
            outfile.write(l)

if __name__=='__main__':
    # manual run, must be in directory of download
    from check_structure import do_check_structure
    
    field=os.path.basename(os.getcwd())
    averaged,dysco=do_check_structure()
    
    make_custom_config(field,'.',True,averaged)
    
