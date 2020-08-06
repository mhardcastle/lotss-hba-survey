#!/usr/bin/python
from astropy.io import fits
import numpy as np
import glob
import sys

# Utility functions for making the cube

def get_freqs_hdus(directory,filenames):
    hdus=[]
    freqs=[]
    g=glob.glob(directory+'/'+filenames)
    for f in g:
        hdus.append(fits.open(f))
        header=hdus[-1][0].header
        if 'CUNIT4' in header and header['CUNIT4']=='Hz':
            freqs.append(header['CRVAL4'])
        else:
            freqs.append(header['RESTFRQ'])
        print f,freqs[-1]

    freqs,hdus = (list(x) for x in zip(*sorted(zip(freqs, hdus), key=lambda pair: pair[0])))
    return freqs,hdus
    
def make_cube(freqs,hdus,outfile):

    chans=[]
    for h in hdus:
        ch,stokes,y,x=h[0].data.shape
        chans.append(ch)
    print chans
        
    newdata=np.zeros((np.sum(chans),stokes,y,x),dtype=np.float32)
    print newdata.shape
    for i,h in enumerate(hdus):
        if i==0:
            chb=0
        else:
            chb=sum(chans[:i])
        print chb,chb+chans[i]
        newdata[chb:chb+chans[i],:,:,:]=h[0].data

    ohdu=hdus[0]
    ohdu[0].data=newdata
    ohdu[0].header['NAXIS4']=np.sum(chans)
    #ohdu[0].header['CTYPE3']='FREQ'
    #ohdu[0].header['CUNIT3']='Hz'
    #ohdu[0].header['CRPIX3']=1
    #ohdu[0].header['CRVAL3']=freqs[0]
    #ohdu[0].header['CDELT3']=freqs[1]-freqs[0]
    hdus[0].writeto(outfile,clobber=True)

if __name__=='__main__':
    directory=sys.argv[1]
    outfile=sys.argv[2]
    freqs,hdus=get_freqs_hdus(directory,'image_full_low_QU_Cube*.cube.dirty.corr.fits')
    make_cube(freqs,hdus,outfile)
    
