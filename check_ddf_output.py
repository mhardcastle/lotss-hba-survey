# example code to do various checks on the ddf-pipeline output
# assume that everything is in ddfsolutions...

import glob
import re
import os
import numpy as np
from scipy.interpolate import InterpolatedUnivariateSpline

path='ddfsolutions/'

def column_used():
    g=sorted(glob.glob(path+'logs/KillMS*DIS2_full.log'))
    infile=g[0]
    with open(infile) as f:
        lines=f.readlines()

    command=lines[0]
    result=re.search('--InCol .*',command)
    if result:
        return result.group().split()[1]
    else:
        return None

def frequencies_present():
    g=glob.glob('*frequencies.txt')
    return len(g)>0

def regenerate_frequencies():
    # look at the killms logs to find the frequencies of the MSes in mslist.txt
    # then write something out which is appropriate
    mslist=[l.rstrip().replace('.archive','') for l in open(path+'/mslist.txt').readlines()]
    obsids = set([os.path.basename(ms).split('_')[0] for ms in mslist])
    print(obsids,mslist)
    for o in obsids:
        print('Doing obsid',o)
        outname=path+o+'frequencies.txt'
        with open(outname,'w') as outfile:
            count=1
            for m in mslist:
                if m.startswith(o):
                    lines=open(path+'logs/KillMS-'+m+'_DIS2_full.log').readlines()
                    for l in lines:
                        if 'Frequency' in l:
                            outfile.write("%s Total_flux_ch%i E_Total_flux_ch%i True\n" % (l.split()[6].replace(']',''),count,count))
                            count+=1
                        

def apply_bootstrap(mslist):
    import pyrap.tables as pt
    
    # Apply the bootstrap correction to a list of measurement sets
    # Recycled code from bootstrap.py
    for ms in mslist:
        print('Doing ms',ms)
        obsid=os.path.baseline(ms).split('_')[0]
        freqfile=path+o+'frequencies.txt'
        lines=open(freqfile).readlines()
        freqs=[]
        for l in lines:
            if 'True' in l:
                freqs.append(float(l.split()[0]))
        scale=np.load(obsid+'crossmatch-results-2.npy')[:,0]
        spl = InterpolatedUnivariateSpline(freqs, scale, k=1)
        t = pt.table(ms)
        try:
            dummy=t.getcoldesc('SCALED_DATA')
        except RuntimeError:
            dummy=None
        t.close()
        if dummy is not None:
            warn('Table '+ms+' has already been corrected, skipping')
        else:
            t = pt.table(ms+'/SPECTRAL_WINDOW', readonly=True, ack=False)
            frq=t[0]['REF_FREQUENCY']
            factor=spl(frq)
            print(frq,factor)
            t=pt.table(ms,readonly=False)
            desc=t.getcoldesc('DATA')
            desc['name']='SCALED_DATA'
            t.addcols(desc)
            d=t.getcol('DATA')
            d*=factor
            t.putcol('SCALED_DATA',d)
            t.close()

if __name__=='__main__':
    print('column used was',column_used())
    print('frequencies_present is',frequencies_present())
    regenerate_frequencies()
    
