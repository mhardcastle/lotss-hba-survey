#!/usr/bin/env python

from __future__ import print_function
from __future__ import absolute_import
import os
import glob
from average import average
from auxcodes import MSList
import numpy as np
from surveys_db import update_status,use_database

def do_check_structure(workdir='.'):

    averaged=False
    g=glob.glob(workdir+'/*.ms')
    msl=MSList(None,mss=g)
    dysco=np.any(msl.dysco)
    if np.sum(msl.hascorrected)<len(msl.mss) and np.sum(msl.hascorrected)!=0:
        if use_database():
            update_status(None,'Check failed')
        raise RuntimeError('Incompatible mix of CORRECTED_DATA and DATA!')
        
    uobsids=set(msl.obsids)
    for thisobs in uobsids:
        # check one MS with each ID
        for m,ch,o,hc in zip(msl.mss,msl.channels,msl.obsids,msl.hascorrected):
            if o==thisobs:
                if not(hc):
                    print('MS',m,'has no corrected_data column, force use of DATA')
                    averaged=True
                channels=len(ch)
                print('MS',m,'has',channels,'channels')
                if channels>20:
                    if use_database():
                        update_status(name,'Averaging',workdir=workdir)
                    print('Averaging needed for',thisobs,'!')
                    averaged=True
                    average(wildcard=workdir+'/*'+thisobs+'*')
                    os.system('rm -r '+workdir+'/*'+thisobs+'*pre-cal.ms')
                break

    return dysco,averaged

if __name__=='__main__':
    do_check_structure()
    
