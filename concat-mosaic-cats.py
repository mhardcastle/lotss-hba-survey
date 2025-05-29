#!/usr/bin/env python

## Original code by T. Shimwell (~?), adapted for updated PyBDSF by B. Barkus (2024). ##

###############
## Imports ##
###############

from __future__ import print_function
from __future__ import division
from builtins import str
from builtins import range
from past.utils import old_div
import os,sys
import glob
from astropy.io import fits
import numpy as np
from astropy import units as u
from astropy.coordinates import SkyCoord
from auxcodes import sepn, getposim             ## LOFAR specific functions from mjh github
import argparse
import time
import random

###############

###############
## Description and Notes ##
###############

# Code to concatenate catalogs made from mosaics, remove repeated sources and to manipulate catalog entries to the final catalog format.
# This code needs to by run (in the signularity container) in the folder you want the outputs to be in.

## To do
# -- Need to finalise the column choices
# -- We probably want to check the masks for each image use those mask islands to determine which mosaic to select the source from as perhaps there  are very extneded sources approximately midway between pointings (especially for gaussian components list)
# -- We probably want an entry in the catalog to say if the source was in mask used for the final deconvolution
# -- Needs speeding up -- each pointing separately
# -- update the errors that are put on the RA and DEC (could use astrometry error maps?)
# -- update the source extension definition

###############

###############
## Variables and Conversions ##
###############

## Angle Conversions Used

arcsec2deg=1.0/3600                                     ## Arcsecond to degrees
#arcmin2deg=1.0/60
deg2rad=old_div(np.pi,180)                              ## Degrees to radians
deg2arcsec = 1.0/arcsec2deg                             ## Degrees to arcseonds
rad2deg=180.0/np.pi                                     ## Radians to degrees
#arcmin2rad=arcmin2deg*deg2rad
#arcsec2rad=arcsec2deg*deg2rad
#rad2arcmin=1.0/arcmin2rad
#rad2arcsec=1.0/arcsec2rad
#steradians2degsquared = (180.0/np.pi)**2.0
#degsquared2steradians = 1.0/steradians2degsquared

## Filename variables

## All these assume a filename of the format "mosaic-blanked"

mosaic = 'mosaic-blanked'                               ## File front
outend = '--final.srl.fits'                             ## Ending needed to convert to the PyBDSF output file
outfull = mosaic+outend
mosend = '.fits'                                        ## Ending required to convert to the mosaic file
gaulend = '--final.gaul.fits'                           ## Ending required to convert to the gaul file

###############

###############
## Functions ##
###############

def concat_catalogs(cats,outconcatcat):

    """
    This function concats the intermediatory catalogues together by creating an empty
    fits data structure and filling this with the data from each of the catalogues, in
    order.

    Parameters
    ----------
    cats:           list
                    The list of intermediatory catalogues to be concat'd into one fits
                    file.

    outconcatcat:   string
                    The name of the final fots file to be outputted.

    Returns
    -------
    outconcatcat:   .fits file
                    The final fits file of all the data from all the catalogues.
    """

    # Use the first catalogue as a dummy and then just update the entries

    f = fits.open(cats[0])                                              ## Open the first catalogue in the list
    nrows = f[1].data.shape[0]                                          ## Obtain the number of rows in the first fits/catalogue
    
    for cat in cats[1:]:                                                ## For the remaining fits files, from 1 onwards
        f2 = fits.open(cat)                                             ## Open files one at a time
        nrows += f2[1].data.shape[0]                                    ## Add the number of rows in these catalogues to the previous total of rows
        f2.close()
    print('Number of rows in first catalogue',nrows)
    
    hdu = fits.BinTableHDU.from_columns(f[1].columns, nrows=nrows)      ## Creates a new fits data set with the same columns but no content as f/catalogue 0

    nrows1 = f[1].data.shape[0]                                         ## Restart the counts of the rows with catalogue 0 in the list
    
    for cat in cats[1:]:                                                ## For all the remaining catalogues
        f2 = fits.open(cat)                                             ## Open the fits files
        nrows2 = nrows1 + f2[1].data.shape[0]                           ## For each add the number of rows to the total number of rows
        for colname in f[1].columns.names:                              ## For each column by name in f
            hdu.data[colname][nrows1:nrows2] = f2[1].data[colname]      ## take the data in that column and insert it into the rows added for this catalogue
        nrows1 += f2[1].data.shape[0]                                   ## Add the number of rows for this catalogue to the total
        f2.close()

    hdu.writeto(outconcatcat,overwrite=True)                            ## Output the final fits file


###############

def find_pointing_coords(mosdirectories):

    """
    This function determines the RAs and DECs (in radians) of the reference pixels
    in each of the mosaics.

    Parameters
    ----------
    mosdirectories: List
                    This is a list of all the directories which each contain one 
                    of the mosaics.

    Returns
    -------
    pointingsras:   List, float
                    The RA values of the reference pixels, taken from the headers of 
                    the mosaic fits files. Converted to radians.
    
    pointingdecs:   List, float
                    The DEC values of the reference pixels, taken from the headers of
                    the mosaic fits files. Converted to radians.
    
    mosaiccats:     List, strings
                    The corresponding paths to each of the mosaics, that match up to
                    the RA and DEC and each of the same position in the pointingra/dec
                    lists.
    """
    mosaiccats = []
    pointingras = np.array([])
    pointingdecs = np.array([])
    
    if len(mosdirectories)==1 and '*' in mosdirectories[0]:                     ## If there are no directories in mosdirectories then take that as the only directory
        mosdirectories=glob.glob(mosdirectories[0])
    
    for directory in mosdirectories:                                            ## For each directory in mosdirectories
        dircats = glob.glob(f'{directory}/{outfull}')                           ## Convert this to match to output of PyBDSF
        for mosaiccat in dircats:
            mosaiccats.append(mosaiccat)                                        ## Append to the list of name changed files

    for mosaiccat in mosaiccats:
        pointing = mosaiccat.replace(outend,mosend)                             ## Change the name to match the main mosaic
        print('Pointing is',pointing)
        print('Opening',pointing)
        f = fits.open(pointing)                                                 ## Open the main mosaic
        print('Finding RA and DEC for',pointing,'in find_pointing_coords')
        pointingras = np.append(pointingras,f[0].header['CRVAL1']*deg2rad)      ## Find the RA of the reference pixel in the mosaic - convert to radians
        pointingdecs = np.append(pointingdecs,f[0].header['CRVAL2']*deg2rad)    ## Find the DEC of the reference pixel in the mosaic - convert to radians
        f.close()
        
    return pointingras,pointingdecs,mosaiccats


###############

def filter_catalogs(pointingras,pointingdecs,mosaiccat,outname,dessourcenums,cattype):

    """
    This function filters the catalogues and creates intermediatory catalogues which then
    get concatenated to make the final catalogues. To do the filtering, each source in the
    mosaics are comapred distance wise to the centres of each mosaic. The sources are then
    kept or filtered depending on whether they are closest to the current mosaic reference
    pixel or not.

    Parameters
    ----------
    pointingras:    List, float
                    This is a list of all the RAs of the reference pixels of the mosaics,
                    in radians.

    pointingdecs:   List, float
                    This is a list of all the DECs of the reference pixels of the moasics,
                    in radians.

    mosaiccats:     List, string
                    This is a list of the paths to the mosaic fits.

    outname:        String
                    This is the filename of the intermediatory catalogue generated.
    
    dessourcenums:  List, int
                    This is a list of the indices which will be kept and used to make up the
                    catalogues. This starts empty for an .srl catalogue and contains the
                    output from the .srl run in a .gaul run.

    cattype:        String
                    This input determines which type of catalogue is being created, a .srl
                    or a .gaul.

    Returns
    -------
    sourcenum:      Array, string
                    This is a numpy array which lists all the Source_IDs for the sources in
                    the filtered catalogues.

    outcat:         .fits
                    This is a fits file catalogue which contains all the sources and their
                    information for each filtered catalogue.
    """

    ## This calculation is not needed and is very slow so has been removed from the code
    ## Once I confirm that the code is all working I will remove these lines
    #if len(pointdirectories)==1 and '*' in pointdirectories[0]:
    #    pointdirectories=glob.glob(pointdirectories[0])

    # astromed = find_median_astrometry(pointdirectories,rapointing,decpointing)
    # if astromed is None:
    #     astromed=5.0 # missing data, assume bad e.g. hole


    ## Setting up the columns for forming the data columns later ##
    sourceids = np.array([])                        ## Source_ID
    sourceresolved = np.array([])                   ## Resolved
    sourcera = np.array([])                         ## RA
    e_sourcera = np.array([])                       ## Error on RA
    #e_sourcera_tot = np.array([])
    sourcedec = np.array([])                        ## DEC
    e_sourcedec = np.array([])                      ## Error on DEC
    #e_sourcedec_tot = np.array([])
    sint = np.array([])                             ## Integrate flux
    e_sint = np.array([])                           ## Error on integrated flux
    #e_sint_tot = np.array([])
    speak = np.array([])                            ## Max flux
    e_speak = np.array([])                          ## Error on max flux
    #e_speak_tot = np.array([])
    maj = np.array([])                              ## Major axis
    e_maj = np.array([])                            ## Error on major axis
    smin = np.array([])                             ## Minor axis?
    e_smin = np.array([])                           ## Error on minor axis?
    dcmaj = np.array([])                            ##
    e_dcmaj = np.array([])                          ##
    dcsmin = np.array([])                           ##
    e_dcsmin = np.array([])                         ##
    pa = np.array([])                               ## Angle of gaussian
    e_pa = np.array([])                             ## Error on angle of gaussian
    dcpa = np.array([])                             ##
    e_dcpa = np.array([])                           ##
    rms_noise = np.array([])                        ## RMS
    stype = np.array([])                            ##
    mosaic_identifier  = np.array([])               ## Identifies the mosaic it came from
    gausid = np.array([])                           ## Gaussian ID
    islid = np.array([])                            ## Island ID
    sourcenum = np.array([])                        ## Source number

    keepindices = []                                                    ## List for the indices to be kept in this catalogue
    time1 = time.time()                                                 ## Start timing the process
    
    pointing = mosaiccat.replace(outend, mosend)                        ## Create a pointing list of the mosaic names
    
    if cattype == 'gaus':                                               ## For the gaus catalogues
        sourcecat = fits.open(mosaiccat)                                ## Look for the .gaul files, fail and error message if they do not exist.
        globstring=mosaiccat.replace(outend,gaulend)
        files=glob.glob(globstring)
        if len(files)==0:
            print('Globstring was',globstring)
            raise RuntimeError('Failed to find gaul file')
        else:
            mosaiccat = files[0]

    print('Opening',pointing)
    f = fits.open(pointing)                                             ## Open and obtain the RA and DEC of the reference pixel for the currently worked on mosaic
    print('Finding RA and DEC in',pointing,'for filter_catalogs')
    rapointing = f[0].header['CRVAL1']*deg2rad
    decpointing = f[0].header['CRVAL2']*deg2rad
    f.close()

    cat = fits.open(mosaiccat)                                          ## Open the PyBDSF output

    closepointingindex = np.where(sepn(pointingras,pointingdecs,rapointing,decpointing)*rad2deg < 5.0)      ## Work out which mosaics are within 5 degrees of the current one
    
    numsources = len(cat[1].data['RA'])                                 ## Determine the number of sources in this catalogue    
    for i in range(0,numsources-1):                                       ## For each source in this mosaic
        
        if len(dessourcenums)==0:                                         ## If there is nothing in the list already (i.e. .srl)
        #if not any(dessourcenums):                                      ## This should remove the DepreciationWarning/Error when it occurs by looking for an empty list/array
            ## Work out all seperations between each source and the RA and DEC of each reference pixel for each mosaic within 5 degs
            allsep = sepn(pointingras[closepointingindex],pointingdecs[closepointingindex],cat[1].data['RA'][i]*deg2rad,cat[1].data['DEC'][i]*deg2rad)
            ## Work out the sepeartion between each source in the mosaic and the mosaic reference pixel
            centsep =  sepn(rapointing,decpointing,cat[1].data['RA'][i]*deg2rad,cat[1].data['DEC'][i]*deg2rad)
            
            if min(allsep) != centsep:                                  ## If the source is closer to another mosiac reference pixel move on to the next bit
                continue
            else:                                                       ## If the source is closest to this mosaic reference pixel, put its index in the list
                keepindices.append(i)
        else:                                                           ## If there are sources in the list
            if cat[1].data['Source_id'][i] in dessourcenums:            ## Check to see if the Source_ID is in the list, if so keep the index
                keepindices.append(i)
            else:                                                       ## Otherwise move to the next bit
                continue
            
        ## This forms the Source_ID name
        if cattype == 'srl':
            sc=SkyCoord(cat[1].data['RA'][i]*deg2rad*u.rad,cat[1].data['DEC'][i]*deg2rad*u.rad,frame='icrs')
        
        if cattype == 'gaus':
            sourceindex=cat[1].data['Source_id'][i]
            sc=SkyCoord(sourcecat[1].data['RA'][sourceindex]*deg2rad*u.rad,sourcecat[1].data['DEC'][sourceindex]*deg2rad*u.rad,frame='icrs')
        s=sc.to_string(style='hmsdms',sep='',precision=3)
        s=sc.to_string(style='hmsdms',sep='',precision=2)
        identity = str('ILTJ'+s).replace(' ','')[:-1]
        sourceids = np.append(sourceids,identity)

        ## To calculate Flux ratios and SNR
        if cattype == 'srl':
            mosaic_identifier = np.append(mosaic_identifier,mosaiccat.split('/')[-2])
        
        if cattype == 'gaus':
            mosaic_identifier = np.append(mosaic_identifier,mosaiccat.split('/')[-5])
        fluxratio = old_div(cat[1].data['Total_flux'][i],cat[1].data['Peak_flux'][i])
        snr  = old_div(cat[1].data['Peak_flux'][i],cat[1].data['Isl_rms'][i])


        # Some equation to figure out if the source is resolved -- leave these dummy values for now.
        if fluxratio > (1.483 + 1000.4/(snr**3.94)):
            #if fluxratio > (1.50341355 + 1.78467767/(snr**0.78385826)):
            sourceresolved = np.append(sourceresolved,'R')
        else:
            sourceresolved = np.append(sourceresolved,'U')

    print('Keeping %s sources for %s -- filtering took %s'%(len(keepindices),pointing,time.time()-time1))

    ## THIS ALL CALCUATES THE COLUMN DATA FOR THE FITS CATALOGUE ##

    sourcera = np.append(sourcera,cat[1].data[keepindices]['RA'])
    e_sourcera = np.append(e_sourcera,cat[1].data[keepindices]['E_RA'])
    #e_sourcera_tot = np.append(e_sourcera_tot,(cat[1].data[keepindices]['E_RA']**2.0 + (astromed*arcsec2deg)**2.0)**0.5) #$ ADD SOME ERROR TO THE SOURCE POSITIONS
    
    sourcedec = np.append(sourcedec,cat[1].data[keepindices]['DEC'])
    e_sourcedec = np.append(e_sourcedec,cat[1].data[keepindices]['E_DEC'])
    #e_sourcedec_tot = np.append(e_sourcedec_tot,(cat[1].data[keepindices]['E_DEC']**2.0 + (astromed*arcsec2deg)**2.0)**0.5) # ADD SOME ERROR TO THE SOURCE POSITIONS
    
    sint = np.append(sint,cat[1].data[keepindices]['Total_flux'])
    e_sint =np.append(e_sint,cat[1].data[keepindices]['E_Total_flux'])
    #e_sint_tot =np.append(e_sint_tot,(cat[1].data[keepindices]['E_Total_flux']**2.0 + (cat[1].data[keepindices]['Total_flux']*0.2)**2.0)**0.5)
    
    speak = np.append(speak,cat[1].data[keepindices]['Peak_flux'])
    e_speak =np.append(e_speak,cat[1].data[keepindices]['E_Peak_flux'])
    #e_speak_tot =np.append(e_speak_tot,(cat[1].data[keepindices]['E_Peak_flux']**2.0 + (cat[1].data[keepindices]['Peak_flux']*0.2)**2.0)**0.5)
    
    maj = np.append(maj,cat[1].data[keepindices]['Maj'])
    e_maj =np.append(e_maj,cat[1].data[keepindices]['E_Maj'])
    smin = np.append(smin,cat[1].data[keepindices]['Min'])
    e_smin = np.append(e_smin,cat[1].data[keepindices]['E_Min'])
    dcmaj = np.append(dcmaj,cat[1].data[keepindices]['DC_Maj'])
    e_dcmaj =np.append(e_dcmaj,cat[1].data[keepindices]['E_DC_Maj'])
    dcsmin = np.append(dcsmin,cat[1].data[keepindices]['DC_Min'])
    e_dcsmin = np.append(e_dcsmin,cat[1].data[keepindices]['E_DC_Min'])
    pa = np.append(pa,cat[1].data[keepindices]['PA'])
    e_pa = np.append(e_pa,cat[1].data[keepindices]['E_PA'])
    dcpa = np.append(dcpa,cat[1].data[keepindices]['DC_PA'])
    e_dcpa = np.append(e_dcpa,cat[1].data[keepindices]['E_DC_PA'])

    rms_noise = np.append(rms_noise,cat[1].data[keepindices]['Isl_rms'])
    stype = np.append(stype,cat[1].data[keepindices]['S_Code'])
    islid = np.append(islid,cat[1].data[keepindices]['Isl_id'])
    sourcenum = np.append(sourcenum,cat[1].data[keepindices]['Source_id'])
    
    col1 = fits.Column(name='Source_Name',format='24A',unit='',array=sourceids)
    col2 = fits.Column(name='RA',format='f8',unit='deg',array=sourcera)
    col3 = fits.Column(name='E_RA',format='f8',unit='arcsec',array=e_sourcera*deg2arcsec)
    #col4 = fits.Column(name='E_RA_tot',format='f8',unit='arcsec',array=e_sourcera_tot*deg2arcsec)
    
    col4 = fits.Column(name='DEC',format='f8',unit='deg',array=sourcedec)
    col5 = fits.Column(name='E_DEC',format='f8',unit='arcsec',array=e_sourcedec*deg2arcsec)
    #col7 = fits.Column(name='E_DEC_tot',format='f8',unit='arcsec',array=e_sourcedec_tot*deg2arcsec)
    
    col6 = fits.Column(name='Peak_flux',format='f8',unit='beam-1 mJy',array=speak*1000.0)
    col7 = fits.Column(name='E_Peak_flux',format='f8',unit='beam-1 mJy',array=e_speak*1000.0)
    #col10 = fits.Column(name='E_Peak_flux_tot',format='f8',unit='beam-1 mJy',array=e_speak_tot*1000.0)
    
    col8 = fits.Column(name='Total_flux',format='f8',unit='mJy',array=sint*1000.0)
    col9 = fits.Column(name='E_Total_flux',format='f8',unit='mJy',array=e_sint*1000.0)
    #col13 = fits.Column(name='E_Total_flux_tot',format='f8',unit='mJy',array=e_sint_tot*1000.0)
    
    #maj[np.where(sourceresolved=='U')] = np.nan
    #e_maj[np.where(sourceresolved=='U')] = np.nan
    #smin[np.where(sourceresolved=='U')] = np.nan
    #e_smin[np.where(sourceresolved=='U')] = np.nan
    #pa[np.where(sourceresolved=='U')] = np.nan
    #e_pa[np.where(sourceresolved=='U')] = np.nan
    
    col10 =  fits.Column(name='Maj',format='f8',unit='arcsec',array=maj*deg2arcsec)
    col11 =  fits.Column(name='E_Maj',format='f8',unit='arcsec',array=e_maj*deg2arcsec)
    
    col12 =  fits.Column(name='Min',format='f8',unit='arcsec',array=smin*deg2arcsec)
    col13 =  fits.Column(name='E_Min',format='f8',unit='arcsec',array=e_smin*deg2arcsec)

    col14 =  fits.Column(name='DC_Maj',format='f8',unit='arcsec',array=dcmaj*deg2arcsec)
    col15 =  fits.Column(name='E_DC_Maj',format='f8',unit='arcsec',array=e_dcmaj*deg2arcsec)
    
    col16 =  fits.Column(name='DC_Min',format='f8',unit='arcsec',array=dcsmin*deg2arcsec)
    col17 =  fits.Column(name='E_DC_Min',format='f8',unit='arcsec',array=e_dcsmin*deg2arcsec)
    
    col18 =  fits.Column(name='PA',format='f8',unit='deg',array=pa)
    col19 =  fits.Column(name='E_PA',format='f8',unit='deg',array=e_pa)

    col20 =  fits.Column(name='DC_PA',format='f8',unit='deg',array=dcpa)
    col21 =  fits.Column(name='E_DC_PA',format='f8',unit='deg',array=e_dcpa)

    #col20 = fits.Column(name='Resolved',format='1A',unit='',array=sourceresolved)
    
    col22 = fits.Column(name='Isl_rms',format='f8',unit='beam-1 mJy',array=rms_noise*1000.0)
    col23 = fits.Column(name='S_Code',format='1A',unit='',array=stype)
    
    col24 = fits.Column(name='Mosaic_ID',format='11A',unit='',array=mosaic_identifier)
    
    #col29 = fits.Column(name='Isl_id',format='I8',unit='',array=islid)
	
    # With unique source names that are matched with source and gaussian catalogs the source_id is not needed.
    #col24 = fits.Column(name='Source_id',format='I8',unit='',array=sourcenum)
    
    ## THIS SECTION FORMS THE FITS FILES ##

    ## Column names are defined
    if cattype == 'gaus':
        gausid = np.append(gausid,cat[1].data[keepindices]['Gaus_id'])
        col25 = fits.Column(name='Gaus_id',format='I8',unit='',array=gausid)

    if cattype == 'srl':    
        cols = fits.ColDefs([col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11,col12,col13,col14,col15,col16,col17,col18,col19,col20,col21,col22,col23,col24])
    if cattype == 'gaus':
        cols = fits.ColDefs([col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11,col12,col13,col14,col15,col16,col17,col18,col19,col20,col21,col22,col23,col24,col25])
    
    tbhdu = fits.BinTableHDU.from_columns(cols)                                     ## Form a fits data structure from these columns

    ## CREATES REGION FILES FOR DS9 ##

    if cattype == 'gaus':
        regionfile = open('%s.gaus.reg'%outname,'w')
    if cattype == 'srl':
        regionfile = open('%s.srl.reg'%outname,'w')
    regionfile.write('# Region file format: DS9 version 4.0\n')
    regionfile.write('global color=green font="helvetica 10 normal" select=1 highlite=1 edit=1 move=1 delete=1 include=1 fixed=0 source\n')
    regionfile.write('fk5\n')
    for i in range(0,len(sourceids)):
        if not np.isnan(maj[i]):
            regionfile.write('ellipse(%s,%s,%s,%s,%s)\n'%(sourcera[i],sourcedec[i],maj[i],smin[i],pa[i]+90))
        else:
            regionfile.write('box(%s,%s,5.0",5.0",0.0)\n'%(sourcera[i],sourcedec[i]))
    regionfile.close()

    ## SAVES FITS FILE OF INTERMEDATORY CATALOGUES ##

    prihdr = fits.Header()
    prihdr['NAME'] = outname
    prihdu = fits.PrimaryHDU(header=prihdr)
    tbhdulist = fits.HDUList([prihdu, tbhdu])
    if cattype == 'srl':
        outcat = outname +'.srl.fits'
    if cattype == 'gaus':
        outcat = outname +'.gaus.fits'
    tbhdulist.writeto(outcat,overwrite=True)

    return sourcenum,outcat


###############

def do_concat(mosdirectories):

    """
    This function concats all the intermediate catalogues together. It is the function that
    checks whether the process has been started by looking for the intermediatory catalogues. If
    it has not then it will proceed to filter catalogues and go from there. If the catalogues do
    exist then it moves straight to the concat part.

    Parameters
    ----------
    mosdirectories:     List, directories, string
                        This is a list of directories that contains the output from PyBDSF.
    
    Returns
    -------
    The final saved concated catalogues via the concat_catalogs function.
    """
    pointingras,pointingdecs,mosaiccats = find_pointing_coords(mosdirectories)

    srlcatnames = []                                                    ## Empty lists for storing the intermediate catalogues
    gauscatnames = []

    random.shuffle(mosaiccats)
    
    for mosaiccat in mosaiccats:
        pointingsourcenums = []
        print('Working on %s'%mosaiccat)
        outname = mosaiccat.split('/')[-2] + 'cat'                      ## Create the output name of the intermediatory catalogue
        if not os.path.exists(outname +'.srl.fits'):                    ## Check to see if it exists
            pointingsourcenums,srlcat = filter_catalogs(pointingras,pointingdecs,mosaiccat,outname,pointingsourcenums,'srl')
        else:
            srlcat = outname + '.srl.fits'
        if not os.path.exists(outname +'.gaus.fits'):                   ## Check to see if it exists
            pointingsourcenums,gauscat = filter_catalogs(pointingras,pointingdecs,mosaiccat,outname,pointingsourcenums,'gaus')
        else:
            gauscat = outname +'.gaus.fits'
            
        srlcatnames.append(srlcat)                                      ## append to output to the lists
        gauscatnames.append(gauscat)
    print('Concatenating %s files'%len(srlcatnames))
    concat_catalogs(srlcatnames,'LoTSS_DR3_rolling.srl.fits')           ## Concat the catalogues in the lists and save
    concat_catalogs(gauscatnames,'LoTSS_DR3_rolling.gaus.fits')

###############

###############
## Run Code ##
###############

if __name__=='__main__':
    parser = argparse.ArgumentParser(description='Concatenate ddf-pipeline mosaic directories')
    parser.add_argument('--mosdirectories', metavar='D', nargs='+',help='mosaic directory name')            ## Argument of the folders containg the PyBDSF output, and the mosaics
    #parser.add_argument('--pointdirectories', metavar='D', nargs='+',help='pointing directory name')
    parser.add_argument('--use-database', help='Use database for DR2 fields', action='store_true')          ## If these directories do not exist
    args = parser.parse_args()

    if not args.use_database:                                                                               ## If you have the directories
        do_concat(args.mosdirectories)#,args.pointdirectories)                                              ## Do the concat
    else:                                                                                                   ## Otherwise make the directories
        from surveys_db import SurveysDB
        with SurveysDB(readonly=True) as sdb:
            sdb.cur.execute('select id from fields where dr3>1 and status="Verified"')
            res=sdb.cur.fetchall()
        mosdirectories=[]
        #pointdirectories=[]
        for r in res:
            id=r['id']
            md=args.mosdirectories[0]+'/'+id
            print(md,md+outfull)
            if os.path.isfile(md+'/'+outfull):
                #pd=args.pointdirectories[0]+'/'+id
                mosdirectories.append(md)
                #pointdirectories.append(pd)
        print(mosdirectories)
        do_concat(mosdirectories)#,pointdirectories)                                                        ## And then do the concat

###############

###############
## Example Call ##
###############

# call as e.g. /home/mjh/pipeline-master/ddf-pipeline/scripts/concat-mosaic-cats.py --mosdirectories=/data/lofar/DR2/mosaics/*  --pointdirectories=/data/lofar/DR2/fields/*

# or concat-mosaic-cats.py --mosdirectories=/data/lofar/DR2/mosaics  --pointdirectories=/data/lofar/DR2/fields --use-database


###############
## Defunct ##
###############

# def find_median_astrometry(astromaps,pointingra,pointingdec):

#     """
#     FUNCTION DEFUNCT AND NOT IN USE
#     """

#     foundpointing = False
#     for astromap in astromaps:
#         amname='%s/astromap.fits'%astromap
#         if not os.path.isfile(amname):
#             continue
#         ra,dec=getposim(amname)
#         if sepn(ra*deg2rad,dec*deg2rad,pointingra,pointingdec)*rad2deg < 0.6:        
#             foundpointing = True
#             f = fits.open('%s/astromap.fits'%astromap)
#             _,_,ys,xs=f[0].data.shape
#             # Use central 20% of the image
#             subim=f[0].data[0][0][old_div(ys,2)-old_div(ys,5):old_div(ys,2)+old_div(ys,5),old_div(xs,2)-old_div(xs,5):old_div(xs,2)+old_div(xs,5)].flatten()
#     if foundpointing:
#         subim = np.nan_to_num(subim)
#         return np.median(subim)
#     else:
#         print('Cant find astrometry near %s, %s'%(pointingra*rad2deg,pointingdec*rad2deg))
#         return None
   

###############
