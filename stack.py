# quick and dirty image plane stack for deep fields
# weight by 1/cent rms**2, otherwise do nothing clever

from __future__ import print_function
from astropy.io import fits
import sys
from auxcodes import get_rms

def stack(files,outname='stack.fits'):
    wsum=0
    for i,f in enumerate(files):
        hdu=fits.open(f)
        rms=get_rms(hdu)
        print(i,f,rms)
        if i==0:
            data=hdu[0].data/rms**2
        else:
            data+=hdu[0].data/rms**2
        wsum+=1/rms**2

    hdu[0].data=data/wsum
    hdu.writeto(outname,overwrite=True)

if __name__=='__main__':
    files=sys.argv[1:]
    stack(files)
