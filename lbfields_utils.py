#!/usr/bin/python

# clone of monitor.py to do monitoring of full field reprocessing

from __future__ import print_function
from time import sleep
import datetime
from surveys_db import SurveysDB, tag_field, get_cluster

## need to update this
#from run_full_field_reprocessing_pipeline import stage_field
#from reprocessing_utils import prepare_field  ## ddf-pipeline utils
import os
import threading
import glob
import requests
import stager_access
from rclone import RClone   ## DO NOT pip3 install --user python-rclone -- use https://raw.githubusercontent.com/mhardcastle/ddf-pipeline/master/utils/rclone.py
from download_file import download_file ## in ddf-pipeline/utils
#import progress_bar
from sdr_wrapper import SDR
from reprocessing_utils import do_sdr_and_rclone_download, do_rclone_download
from tasklist import *
from calibrator_utils import *
from plot_field import *
import numpy as np


##############################
## do things by obsid

def get_obsids( name, survey=None ):
    with SurveysDB(survey=survey) as sdb:
        sdb.execute('select * from observations where field="'+name+'"')
        fld = sdb.cur.fetchall()
    obsids = [ val['id'] for val in fld ]
    return(obsids)

def get_local_obsid( name ):
    basedir = os.getenv('LINC_DATA_DIR')
    fielddir = os.path.join(basedir, name)
    fieldfiles = glob.glob(os.path.join(fielddir,'*'))
    fielddirs = []
    for ff in fieldfiles:
        tmp = os.path.splitext(ff)
        if tmp[1] == '':
            tmp2 = os.path.basename(tmp[0])
            if tmp2.isdigit():
                fielddirs.append(tmp2)
    return(fielddirs)

##############################
## finding and checking solutions 

def collect_solutions( caldir ):
    survey=None
    obsid = os.path.basename(caldir)
    namedir = os.path.dirname(caldir)
    name = os.path.basename(namedir)
    tasklist = []

    with SurveysDB(survey=survey) as sdb:
        sdb.execute('select * from observations where id="'+obsid+'"')
        fld = sdb.cur.fetchall()
    calibrator_id = fld[0]['calibrator_id']

    ## check if linc/prefactor 3 has been run
    linc_check = get_linc( obsid, caldir )

    if linc_check: 
        ## get time last modified to compare with ddfpipeline (pref1 vs pref3 tests means some pref3 were run after ddfpipeline)
        linc_time = os.path.getmtime(os.path.join(caldir,'LINC-target_solutions.h5'))
        ## find the ddf solutions
        soldir = os.path.join(caldir,'ddfsolutions')
        if not os.path.exists(soldir):
            os.mkdir(soldir)
        do_sdr_and_rclone_download(name,soldir,verbose=False,Mode="Misc",operations=['download'])
        cwd = os.getcwd()
        os.chdir(soldir)
        os.system('tar -xvf misc.tar *crossmatch-results-2.npy')
        os.chdir(cwd)
        ddfpipeline_time = os.path.getmtime(glob.glob(os.path.join(soldir,'*crossmatch-results-2.npy'))[0])

        if ddfpipeline_time - linc_time > 0:
            ## linc was run before ddfpipeline -- in this case can start with vlbi pipeline directly
            ## download the rest of the ddfpipeline things
            do_sdr_and_rclone_download(name,soldir,verbose=False,Mode="Imaging",operations=['download'])
            cwd = os.getcwd()
            os.chdir(soldir)
            os.system('tar -xvf images.tar image_full_ampphase_di_m.NS.app.restored.fits')
            os.system('tar -xvf images.tar image_full_ampphase_di_m.NS.mask01.fits')
            os.system('tar -xvf images.tar image_full_ampphase_di_m.NS.DicoModel')
            os.system('tar -xvf uv.tar image_dirin_SSD_m.npy.ClusterCat.npy')
            os.system('tar -xvf uv.tar SOLSDIR')
            os.system('tar -xvf misc.tar logs/*DIS2*log')
            os.system('tar -xvf misc.tar L*frequencies.txt')
            os.chdir(cwd)
            ## what's needed is actually just:
            ## DDS3_full_slow*.npz 
            ## DDS3_full*smoothed.npz 
            ## but SOLSDIR needs to be present with the right directory structure, this is expected for the subtract.
            ## and the bootstrap if required
            '''
            So the things needed for the subtract are:
            and if the bootstrap is applied
            L*frequencies.txt (which can probably be reconstructed if missing)


            DIS2 logs --> look for the input column and if that's scaled data then you NEED the numpy array. for versions that start from data then those values are absorbed into the amplitude solutions and need to re-run
            and if the input col is scaled data then need to check for the numpy array

            scaled data + no numpy array = rerun up to boostrap
            scaled data + numpy array = need to generate data (i.e. scaled with numpy corrections) [there is code]

            frequencies missing --> regenerate from small mslist  [there is code]
            '''
            tasklist.append('setup')
            tasklist.append('concat-flag')
            tasklist.append('phaseup-concat')
            tasklist.append('delay')
            tasklist.append('split')
            tasklist.append('selfcal')
        else:
            ## linc was run after and ddfpipeline ("light" options) need to be run
            tasklist.append('setup')
            tasklist.append('concat-flag')
            tasklist.append('ddflight') ## needs to include initial dumping of intl stations
            ## apply solutions?
            tasklist.append('phaseup-concat')
            tasklist.append('delay')
            tasklist.append('split')
            tasklist.append('selfcal')
    else:
        print('valid LINC solutions not found. Checking lb_calibrators.')
        ## linc is not good
        result = download_field_calibrators(name,caldir)
        solutions = unpack_calibrator_sols(caldir,result)
        if len(solutions) >= 1:
            print('One or more calibrator found, comparing solutions ...')
            best_sols = compare_solutions(solutions)
            print('Best solutions are {:s}, cleaning up others.'.format(best_sols[0]))
            os.system('cp {:s} {:s}/LINC-cal_solutions.h5'.format(best_sols[0],os.path.dirname(best_sols[0])))
            for sol in solutions:
                os.system('rm -r {:s}/{:s}*'.format(os.path.dirname(best_sols[0]),os.path.basename(sol).split('_')[0]))
            tasklist.append('target')
            ## check if need full ddfpipeline or ddflight? -- talk to tim
            tasklist.append('ddfpipeline')
            tasklist.append('setup')
            tasklist.append('concat-flag')
            tasklist.append('phaseup-concat')
            tasklist.append('delay')
            tasklist.append('split')
            tasklist.append('selfcal')
        else:
            ## need to re-run calibrator .... shouldn't ever be in this situation!
            tasklist.append('calibrator')
            tasklist.append('target')
            tasklist.append('ddfpipeline')
            tasklist.append('setup')
            tasklist.append('concat-flag')
            tasklist.append('phaseup-concat')
            tasklist.append('delay')
            tasklist.append('split')
            tasklist.append('selfcal')
    ## set the task list in the lb_operations table
    set_task_list(obsid,tasklist)

##############################
## staging

def stage_field( name, survey=None ):
    with SurveysDB(survey=survey) as sdb:
        idd = sdb.db_get('lb_fields',name)
    ## currently srmfile is 'multi' if the field has more than one observation, so this staging will fail if that is the case
    srmfilename = idd['srmfile']
    if srmfilename == 'multi':
        obsids = get_obsids(name)
        ## NEED TO UPDATE FOR MULTI FIELDS
    response = requests.get(srmfilename) 
    data = response.text
    uris = data.rstrip('\n').split('\n')
    ## get obsid and create a directory
    obsid = uris[0].split('/')[-2]
    tmp = os.path.join(str(os.getenv('LINC_DATA_DIR')),str(name))
    caldir = os.path.join(tmp,obsid)   
    os.makedirs(caldir) 
    stage_id = stager_access.stage(uris)
    update_status(name, 'Staging', stage_id=stage_id )
    return(caldir)

##############################
## downloading

def do_download( name ):
    update_status(name,'Downloading')
    ## get the staging id from the surveys database
    with SurveysDB(readonly=True) as sdb:
        idd=sdb.db_get('lb_fields',name)
        stage_id = idd['staging_id']
    ## get the surls from the stager API
    surls = stager_access.get_surls_online(stage_id)
    project = surls[0].split('/')[-3]
    obsid = surls[0].split('/')[-2]
    obsid_path = os.path.join(project,obsid)
    if len(surls) > 0:
        tmp = os.path.join(str(os.getenv('LINC_DATA_DIR')),str(name))
        caldir = os.path.join(tmp,obsid)
        ## os.makedirs(caldir)  # now done in stage_field
        if 'juelich' in surls[0]:
            for surl in surls:
                dest = os.path.join(caldir,os.path.basename(surl))
                os.system('gfal-copy {:s} {:s} > {:s}_gfal.log 2>&1'.format(surl.replace('srm://lofar-srm.fz-juelich.de:8443','gsiftp://lofar-gridftp.fz-juelich.de:2811'),dest,name))
            os.system('rm {:s}_gfal.log'.format(name))
        if 'psnc' in surls[0]:
            for surl in surls:
                dest = os.path.join(caldir,os.path.basename(surl))
                os.system('gfal-copy {:s} {:s} > {:s}_gfal.log 2>&1'.format(surl.replace('srm://lta-head.lofar.psnc.pl:8443','gsiftp://gridftp.lofar.psnc.pl:2811'),dest,name))
            os.system('rm {:s}_gfal.log'.format(name))
        if 'sara' in surls[0]:
            ## can use a macaroon
            files = [ os.path.basename(val) for val in surls ]
            macaroon_dir = os.getenv('MACAROON_DIR')        
            lta_macaroon = glob.glob(os.path.join(macaroon_dir,'*LTA.conf'))[0]
            rc = RClone( lta_macaroon, debug=True )
            rc.get_remote()
            #d = rc.multicopy(rc.remote+obsid_path,files,caldir)
            for f in files:
                d = rc.execute(['-P','copy',rc.remote + os.path.join(obsid_path,f)]+[caldir]) 
            if d['err'] or d['code']!=0:
                update_status(field,'rclone failed')
                print('Rclone failed for field {:s}'.format(field))
        ## check that everything was downloaded
        tarfiles = glob.glob(os.path.join(caldir,'*tar'))
        if len(tarfiles) == len(surls):
            print('Download successful for {:s}'.format(name) )
            update_status(id,'Downloaded',stage_id=0)
    else:
        print('SURLs do not appear to be online for {:s} (staging id {:s})'.format(name,str(stage_id)))
        update_status(name,'Download failed')

##############################
## unpacking

def dysco_compress(caldir,msfile):
    msfile = os.path.join(caldir,msfile)
    success=True
    with open(os.path.join(caldir,'dysco_compress.parset'),'w') as f:
        f.write('msin={:s}\n'.format(msfile))
        f.write('msin.datacolumn=DATA\n')
        f.write('msout={:s}.tmp\n'.format(msfile))
        f.write('msout.datacolumn=DATA\n')
        f.write('msout.storagemanager=dysco\n')
        f.write('steps=[count]')
    sing_img = os.getenv('LOFAR_SINGULARITY')
    os.system('singularity exec -B {:s} {:s} DP3 {:s}'.format(os.getcwd(),sing_img,os.path.join(caldir,'dysco_compress.parset')))
    if os.path.exists('{:s}.tmp'.format(msfile)):
        os.system('rm -r {:s}'.format(msfile))
        os.system('mv {:s}.tmp {:s}'.format(msfile,msfile))
    else:
        print('something went wrong with dysco compression for {:s}'.format(msfile))
        success=False
    return(success)

def dysco_compress_job(caldir):
    success=True
    os.system('ls -d {:s}/*.MS > {:s}/myfiles.txt'.format(caldir,caldir))
    file_number = len(open("{:s}/myfiles.txt".format(caldir), "r").readlines())
    command = 'sbatch -W --array=1-{:n}%%30 slurm/{:s}_dysco.sh {:s}'.format(file_number,cluster,caldir)
    if os.system(command):
        print("Something went wrong with the dysco compression job!")
        success = False
    os.system('rm {:s}/myfiles.txt'.format(caldir))
    return success

    
def do_unpack(field):
    update_status(field,'Unpacking')
    success=True
    do_dysco=False # Default should be false
    caldir = os.path.join(str(os.getenv('LINC_DATA_DIR')),field)
    obsdirs = glob.glob(os.path.join(caldir,'*'))
    obsdir = [ val for val in obsdirs if 'csv' not in val ]
    if len(obsdir) > 1:
        ## there are multiple observations for this field, this isn't handled yet
        pass
    else:
        obsdir = obsdir[0]
    ## get the tarfiles
    tarfiles = glob.glob(os.path.join(obsdir,'*tar'))
    ## check if needs dysco compression
    gb_filesize = os.path.getsize(tarfiles[0])/(1024*1024*1024)
    if gb_filesize > 40.:
        do_dysco = True
    if os.getenv("UNPACK_AS_JOB"):
        # Logic for Unpacking Jobs - Files should be named {cluster}_untar.sh and {cluster}_dysco.sh
        for trf in tarfiles:
            os.system('sbatch -W slurm/{:s}_untar.sh {:s} {:s}'.format(cluster, trf, field))
            msname = '_'.join(os.path.basename(trf).split('_')[0:-1])
            os.system( 'mv {:s} {:s}'.format(msname,obsdir))
        if do_dysco:
            dysco_success = dysco_compress_job(obsdir)
    else:
        for trf in tarfiles:
            os.system( 'tar -xvf {:s} >> {:s}_unpack.log 2>&1'.format(trf,field) )
            msname = '_'.join(os.path.basename(trf).split('_')[0:-1])
            os.system( 'mv {:s} {:s}'.format(msname,obsdir))
            if do_dysco:
                dysco_success = dysco_compress(obsdir,msname)
                ## ONLY FOR NOW
                if dysco_success:
                    os.system('rm {:s}'.format(trf))
                
    ## check that everything unpacked
    msfiles = glob.glob('{:s}/L*MS'.format(obsdir))
    if len(msfiles) == len(tarfiles):
        update_status(field,'Unpacked')
        os.system('rm {:s}/*.tar'.format(obsdir))
        os.system('rm {:s}_unpack.log'.format(field))
    else:
        update_status(field,'Unpack failed')

##############################
## verifying

def check_field(field):
    procdir = os.path.join(str(os.getenv('LINC_DATA_DIR')),'processing')
    outdir = os.path.join(procdir,field)
    ## get status from finished.txt
    with open(os.path.join(outdir,'finished.txt'),'r') as f:
        lines = f.readlines()
    if 'SUCCESS: Pipeline finished successfully' in lines[0]:
        success = True
    else:
        print('Pipeline did not report finishing successfully. Please check processing for {:s}!!'.format(field))
        success = False
    ## get the workflow that was run
    with open(os.path.join(outdir,'job_output.txt'),'r') as f:
        lines = [next(f) for _ in range(10)]
    line = [ line for line in lines if 'Resolved' in line ]
    tmp = line[0].split('.cwl')
    workflow = os.path.basename(tmp[0])
    ## get the obsid
    jsonfile = glob.glob(os.path.join(outdir,'mslist*json'))[0]
    with open(jsonfile,'r') as f:
        lines = [next(f) for _ in range(10)]
    line = [ line for line in lines if 'path' in line ]
    tmp = line[0].split('_SB')
    obsid = os.path.basename(tmp[0]).replace('L','')
    return success, workflow, obsid

def do_verify(field):
    tarfile = glob.glob(field+'*tgz')[0]
    macaroon_dir = os.getenv('MACAROON_DIR')
    macaroon = glob.glob(os.path.join(macaroon_dir,'*lofarvlbi_upload.conf'))[0]
    rc = RClone( macaroon, debug=True )
    rc.get_remote()
    d = rc.execute_live(['-P', 'copy', tarfile]+[rc.remote + '/' + 'disk/surveys/'])
    if d['err'] or d['code']!=0:
        update_status(field,'rclone failed')
        print('Rclone failed for field {:s}'.format(field))
    else:
        print('Tidying uploaded directory for',field)
        update_status(field,'Complete')
        ## delete the directory
        os.system( 'rm -r {:s}'.format(os.path.join(procdir,field)))
        ## delete the initial data
        os.system( 'rm -r {:s}'.format(os.path.join(basedir,field)))
        ## delete the tarfile
        os.system( 'rm {:s}.tgz'.format(field))

