#!/usr/bin/env python

# Subtract and vlow image script
# Based on an idea by Martijn Oei

from __future__ import print_function
import os
import glob
from auxcodes import run
from make_vlow_stokesI import update_status,die

def subtract_sources():
    update_status(None,'Subtracting')
    run('sub-sources-outside-region.py -b fullfield -p SUB -c DATA --uselowres -t 8 -f 5')
    update_status(None,'Subtracted')

# wsclean call directly from Martijn's script but matched to 'vlow' scales for ddf-pipeline
    
def run_wsclean(useIDG=False,stringMultiscaleScales = "0,4,8,16,32,64,128,256", stringMultiscaleScalesIDG = "0,4,8,16,32,64",
                sizePixels=2000,taperGaussian=60.0,beamsize=60.0,scale=15.0,numberOfSubbands=6,uvDistanceMinLambda=0,IDGMode="cpu",name='WSCLEAN_low'):

    update_status(None,'WSCLEAN')
    g=glob.glob('*.archive0')
    stringMSs=' '.join(g)
    print("Imaging with multiscale CLEAN (at low-resolution)...")
    if (useIDG):
        # Generate IDG configuration file. Note that we need 'directoryPointingOwn' here.
        print("Generating IDG configuration file...")
        with open(directoryPointingOwn + "aconfig.txt", "w") as configurationFile:
            configurationFile.write("aterms=[beam]\nbeam.differential = true\nbeam.update_interval = 600\nbeam.usechannelfreq = true")

        command = "wsclean -no-update-model-required -size " + str(sizePixels) + " " + str(sizePixels) + " -reorder -weight briggs -0.5 -weighting-rank-filter 3 -clean-border 1 -mgain 0.8 -no-fit-beam -data-column DATA -join-channels -channels-out " + str(numberOfSubbands) + " -padding 1.2 -multiscale -multiscale-scales " + stringMultiscaleScalesIDG + " -auto-mask 1.5 -auto-threshold 1.4 -taper-gaussian " + str(taperGaussian) + "arcsec -circular-beam -beam-size " + str(beamSize) + "arcsec -pol i -name " + name + " -scale " + str(scale) + "arcsec -niter 100000 -minuv-l " + str(uvDistanceMinLambda) + " -use-idg -idg-mode " + IDGMode + " -aterm-kernel-size 16 -aterm-config aconfig.txt " + stringMSs
    else:
        command = "wsclean -no-update-model-required -apply-primary-beam -size " + str(sizePixels) + " " + str(sizePixels) + " -reorder -weight briggs -0.5 -weighting-rank-filter 3 -clean-border 1 -mgain 0.8 -fit-beam -data-column DATA -join-channels -channels-out " + str(numberOfSubbands) + " -padding 1.2 -multiscale -multiscale-scales " + stringMultiscaleScales + " -auto-mask 3.0 -auto-threshold 2.5 -taper-gaussian " + str(taperGaussian) + "arcsec -pol i -name " + name + " -scale " + str(scale) + "arcsec -niter 100000 -minuv-l " + str(uvDistanceMinLambda) + " -baseline-averaging 10.0 " + stringMSs

    print(command)
    run(command)
    update_status(None,'WSCLEAN complete')
    
if __name__=='__main__':
    import sys
    
    field=sys.argv[2]
    os.chdir('/beegfs/car/mjh/vlow/'+field)
    if sys.argv[1]=='subtract':
        subtract_sources()
    elif sys.argv[1]=='clean':
        run_wsclean()
    else:
        raise RuntimeError('Failed to parse command')
    
    
    
