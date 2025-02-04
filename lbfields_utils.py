#!/usr/bin/python

# Utilities for monitor_lbfields and friends

from __future__ import print_function
from time import sleep
import datetime
from surveys_db import SurveysDB, tag_field, get_cluster

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


def update_status(name,status,stage_id=None,time=None,workdir=None,av=None,survey=None):
    # adapted from surveys_db
    # utility function to just update the status of a field
    # name can be None (work it out from cwd), or string (field name)
    with SurveysDB(survey=survey) as sdb:
        idd=sdb.db_get('lb_fields',name)
        if idd is None:
          raise RuntimeError('Unable to find database entry for field "%s".' % name)
        idd['status']=status
        tag_field(sdb,idd,workdir=workdir)
        if time is not None and idd[time] is None:
            idd[time]=datetime.datetime.now()
        if stage_id is not None:
            idd['staging_id']=stage_id
        sdb.db_set('lb_fields',idd)

##############################
## do things by obsid

def get_obsids( name, survey=None ):
    with SurveysDB(survey=survey) as sdb:
        sdb.execute('select * from observations where field="'+name+'"')
        fld = sdb.cur.fetchall()
    obsids = [ val['id'] for val in fld ]
    return(obsids)

def get_local_obsid( name ):
    basedir = os.getenv('DATA_DIR')
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

def collect_solutions_lhr( caldir ):
    survey=None
    obsid = os.path.basename(caldir)
    namedir = os.path.dirname(caldir)
    name = os.path.basename(namedir)
    tasklist = []

    update_status( name,'Solutions' )
    success = True

    with SurveysDB(survey=survey) as sdb:
        sdb.execute('select * from observations where id="'+obsid+'"')
        fld = sdb.cur.fetchall()
    calibrator_id = fld[0]['calibrator_id']

    ## check if linc/prefactor 3 has been run
    linc_check, macname = get_linc( obsid, caldir )    
    if linc_check: 
        tasklist.append('setup')
        tasklist.append('concatenate-flag')
        tasklist.append('phaseup-concat')
        tasklist.append('delay')
        tasklist.append('split')
        tasklist.append('selfcal')
    else:
        print('valid LINC solutions not found. Checking lb_calibrators.')
        ## linc is not good
        result = download_field_calibrators(obsid,caldir)
        solutions = unpack_calibrator_sols(caldir,result)
        if len(solutions) >= 1:
            print('One or more calibrator found, comparing solutions ...')
            best_sols = compare_solutions(solutions)
            print('Best solutions are {:s}, cleaning up others.'.format(best_sols[0]))
            os.system('cp {:s} {:s}/LINC-cal_solutions.h5'.format(best_sols[0],os.path.dirname(best_sols[0])))
            for sol in solutions:
                os.system('rm -r {:s}/{:s}*'.format(os.path.dirname(best_sols[0]),os.path.basename(sol).split('_')[0]))
            tasklist.append('linc-vlbi')
            tasklist.append('concatenate-flag')
            tasklist.append('phaseup-concat')
            tasklist.append('delay')
            tasklist.append('split')
            tasklist.append('selfcal')
        else:
            ## need to re-run calibrator .... shouldn't ever be in this situation but here fore completeness
            success = False
            tasklist.append('calibrator')
            tasklist.append('linc-vlbi')
            tasklist.append('concatenate-flag')
            tasklist.append('phaseup-concat')
            tasklist.append('delay')
            tasklist.append('split')
            tasklist.append('selfcal')
    if success:
        ## set the task list in the lb_operations table
        set_task_list(obsid,tasklist)
        ## set the status back to staging if no runtime errors
        update_status( name, 'Staging' )


def collect_solutions( caldir ):
    survey=None
    obsid = os.path.basename(caldir)
    namedir = os.path.dirname(caldir)
    name = os.path.basename(namedir)
    tasklist = []

    update_status( name,'Solutions' )
    success = True

    with SurveysDB(survey=survey) as sdb:
        sdb.execute('select * from observations where id="'+obsid+'"')
        fld = sdb.cur.fetchall()
    calibrator_id = fld[0]['calibrator_id']

    ## check if linc/prefactor 3 has been run - for calibrator solutions - which are contained in LINC-target_solutions.h5
    linc_check, macname = get_linc( obsid, caldir )

    ## download previous ddfpipeline for re-running
    templatedir = os.path.join(caldir,'ddfpipeline/template')
    os.makedirs(templatedir)
    try:
        download_ddfpipeline_solutions(name,templatedir,ddflight=True)
    except RuntimeError:
        success = False

    if linc_check: 
        ## rename linc solutions so linc-vlbi will pick them up -- but what will happen because the target solutions already exist?
        os.system('cp {:s}/LINC-target_solutions.h5 {:s}/LINC-cal_solutions.h5'.format(caldir,caldir))
        tasklist.append('linc-vlbi')
        tasklist.append('ddflight')
        tasklist.append('concatenate-flag')
        tasklist.append('process-ddf')
        tasklist.append('phaseup-concat')
        tasklist.append('delay')
        tasklist.append('split')
        tasklist.append('selfcal') 
        ## some more widefield things .... 
    else:
        print('valid LINC solutions not found. Checking lb_calibrators.')
        ## linc is not good
        result = download_field_calibrators(obsid,caldir)
        solutions = unpack_calibrator_sols(caldir,result)
        if len(solutions) >= 1:
            print('One or more calibrator found, comparing solutions ...')
            best_sols = compare_solutions(solutions)
            print('Best solutions are {:s}, cleaning up others.'.format(best_sols[0]))
            os.system('cp {:s} {:s}/LINC-cal_solutions.h5'.format(best_sols[0],os.path.dirname(best_sols[0])))
            for sol in solutions:
                os.system('rm -r {:s}/{:s}*'.format(os.path.dirname(best_sols[0]),os.path.basename(sol).split('_')[0]))
            tasklist.append('linc-vlbi')
            tasklist.append('ddflight')
            tasklist.append('concatenate-flag')
            tasklist.append('process-ddf')
            tasklist.append('phaseup-concat')
            tasklist.append('delay')
            tasklist.append('split')
            tasklist.append('selfcal')
            ## some more widefield things ....
        else:
            ## need to re-run calibrator .... shouldn't ever be in this situation!
            success = False
            tasklist.append('calibrator')
            tasklist.append('linc-vlbi')
            tasklist.append('ddflight')
            tasklist.append('concatenate-flag')
            tasklist.append('process-ddf')
            tasklist.append('phaseup-concat')
            tasklist.append('delay')
            tasklist.append('split')
            tasklist.append('selfcal')
    if success:
        ## set the task list in the lb_operations table
        set_task_list(obsid,tasklist)
        ## set the status back to staging if no runtime errors
        update_status( name, 'Staging' )

##############################
## staging

def stage_field( name, survey=None ):
    with SurveysDB(survey=survey) as sdb:
        idd = sdb.db_get('lb_fields',name)
    ## currently srmfile is 'multi' if the field has more than one observation
    srmfilename = idd['srmfile']
    if srmfilename == 'multi':
        with SurveysDB(survey=survey) as sdb:
            sdb.cur.execute('select * from observations where field=%s',(name,))
            obs = sdb.cur.fetchall()
        obs_ok = 0
        # check how many observations there really are, as opposed to failed on
        for nobs in range(len(obs)):
            if obs[nobs]['status'] in ['Archived','DI_Processed']:
                obs_ok+=1
                nobs_ok=nobs
        # If really only 1 observation, construct its srmfile and proceed
        if obs_ok==1:
            srmfilename = 'https://public.spider.surfsara.nl/project/lotss/shimwell/LINC/srmfiles/srm%d.txt'%(obs[nobs_ok]['id'])
        # multiple obs in 1 field complicated because ddflight, phaseup_concat need
        # data from all obs to be done at once; these to be left "for later"
        else:
            print ('Multiple observations in one field not currently supported')
            print ('Updating status to Multiple')
            update_status (name, 'Multiple')
            return(None)
    response = requests.get(srmfilename) 
    data = response.text
    uris = data.rstrip('\n').split('\n')
    ## get obsid and create a directory
    obsid = uris[0].split('/')[-2]
    tmp = os.path.join(str(os.getenv('DATA_DIR')),str(name))
    caldir = os.path.join(tmp,obsid)
    ## if directory already exists, it is possible that some things have already been staged
    if os.path.exists(caldir):
        tarfiles = glob.glob(os.path.join(caldir,'*.tar'))
        trfs = [ val.split('/')[-1] for val in tarfiles ]
        new_uris = [ uri for uri in uris if uri.split('/')[-1] not in trfs ]
        uris = new_uris
    else:
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
        tmp = os.path.join(str(os.getenv('DATA_DIR')),str(name))
        caldir = os.path.join(tmp,obsid)
        ## os.makedirs(caldir)  # now done in stage_field
        if 'juelich' in surls[0]:
            print('Juelich download:',surls[0])
            if 'NO_GRID' in os.environ:
                logfile=None
                prefix="https://lofar-download.fz-juelich.de/webserver-lofar/SRMFifoGet.py?surl="
                for surl in surls:
                    dest = os.path.join(caldir,os.path.basename(surl))
                    if not os.path.isfile(dest):
                        download_file(prefix+surl,dest,retry_partial=True,progress_bar=True,catch_codes=(500,403),retry_size=1024)
                    else:
                        print(dest,'exists already, not downloading')
            else:
                for surl in surls:
                    dest = os.path.join(caldir,os.path.basename(surl))
                    if not os.path.isfile(dest):
                        os.system('gfal-copy {:s} {:s} > {:s}_gfal.log 2>&1'.format(surl.replace('srm://lofar-srm.fz-juelich.de:8443','gsiftp://lofar-gridftp.fz-juelich.de:2811'),dest,name))
                os.system('rm {:s}_gfal.log'.format(name))
        if 'psnc' in surls[0]:
            print('Poznan download:',surls[0])
            if 'NO_GRID' in os.environ:
                auth=None
                # Poznan requires username and password, which are in your .stagingrc
                # Rudimentary parsing of this needed...
                if os.path.isfile(home+'/.stagingrc'):
                    user=None
                    password=None
                    with open(home+'/.stagingrc','r') as f:
                        lines = f.readlines()
                    for l in lines:
                        if '=' in l:
                            bits=l.split('=')
                            if bits[0]=='user': user=bits[1].rstrip('\n')
                            if bits[0]=='password': password=bits[1].rstrip('\n')
                    if user and password:
                        auth=(user,password)
                    else:
                        print('*** Warning: failed to parse stagingrc, download will fail ***')
                logfile=None
                prefix="https://lta-download.lofar.psnc.pl/lofigrid/SRMFifoGet.py?surl="
                for surl in surls:
                    dest = os.path.join(caldir,os.path.basename(surl))
                    if not os.path.isfile(dest):
                        download_file(prefix+surl,dest,retry_partial=True,progress_bar=True,retry_size=1024,auth=auth)
                    else:
                        print(dest,'exists already, not downloading')
            else:
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
        tarfiles = check_tarfiles( caldir )
        if len(tarfiles) == len(surls):
            print('Download successful for {:s}'.format(name) )
            update_status(name,'Downloaded',stage_id=0)
    else:
        print('Something went wrong with the download for {:s} (staging id {:s})'.format(name,str(stage_id)))
        update_status(name,'Download failed')

def check_tarfiles( caldir ):
    trfs = glob.glob(os.path.join(caldir,'*tar'))
    for trf in trfs:
        os.system( 'tar -tvf {:s} > tmp.txt 2>&1'.format(trf) )
        with open( 'tmp.txt', 'r' ) as f:
            lines = f.readlines()
        if 'tar: Unexpected EOF in archive\n' in lines:
            os.system( 'rm {:s}'.format(trf) )
        if 'tar: Exiting with failure status due to previous errors\n' in lines:
            os.system( 'rm {:s}'.format(trf) )
    os.system('rm tmp.txt')    
    trfs = glob.glob(os.path.join(caldir,'*tar'))
    return(trfs)

def get_juelich_macaroon( field ):
    ## get project name
    with SurveysDB(readonly=True) as sdb:
        idd=sdb.db_get('lb_fields',name)
        stage_id = idd['staging_id']
    surls = stager_access.get_surls_online(stage_id)
    tmp = surls[0].split('projects/')
    proj_name = tmp[-1].split('/')[0]
    ## generate voms-proxy-init
    os.system( 'cat ~/macaroons/secret-file | voms-proxy-init --pwstdin --voms lofar:/lofar/user/sksp --valid 1680:0' )
    os.system( 'get-macaroon --url https://dcache-lofar.fz-juelich.de:2880/pnfs/fz-juelich.de/data/lofar/ops/projects/{:s} --duration P7D --proxy --permissions READ_METADATA,DOWNLOAD --ip 0.0.0.0/0 --output rclone {:s}_juelich'.format( proj_name, proj_name ) )
    mac_name = '{:s}_juelich.conf'.format(proj_name) 
    return( mac_name )

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
    cluster = os.getenv('DDF_PIPELINE_CLUSTER')
    success=True
    os.system('ls -d {:s}/*.MS > {:s}/myfiles.txt'.format(caldir,caldir))
    file_number = len(open("{:s}/myfiles.txt".format(caldir), "r").readlines())
    command = 'sbatch -W --array=1-{:n}%%30 {:s} {:s}/lotss-hba-survey/slurm/{:s}_dysco.sh {:s}'.format(file_number,os.getenv('CLUSTER_OPTS'),os.getenv('SOFTWAREDIR'),cluster,caldir)
    if os.system(command):
        print("Something went wrong with the dysco compression job!")
        success = False
    os.system('rm {:s}/myfiles.txt'.format(caldir))
    return success

def do_unpack(field):
    update_status(field,'Unpacking')
    success=True
    do_dysco=False # Default should be false
    caldir = os.path.join(str(os.getenv('DATA_DIR')),field)
    obsdirs = glob.glob(os.path.join(caldir,'*'))
    obsdir = [ val for val in obsdirs if os.path.isdir(val) ]
    if len(obsdir) > 1:
        ## there are multiple observations for this field, this isn't handled yet
        pass
    else:
        obsdir = obsdir[0]
    ## get the tarfiles
    tarfiles = glob.glob(os.path.join(obsdir,'*tar'))
    ## check if needs dysco compression
    gb_filesize = os.path.getsize(tarfiles[0])/(1024*1024*1024)
    ## update the above to be non-hacky
    if gb_filesize > 40.:
        do_dysco = True
    if os.getenv("UNPACK_AS_JOB"):
        # Logic for Unpacking Jobs - Files should be named {cluster}_untar.sh and {cluster}_dysco.sh
        cluster = os.getenv('DDF_PIPELINE_CLUSTER')
        for trf in tarfiles:
            os.system('sbatch {:s} -W {:s}/lotss-hba-survey/slurm/{:s}_untar.sh {:s} {:s}'.format(os.getenv('CLUSTER_OPTS'),os.getenv("SOFTWAREDIR"),cluster, trf, field))
            #msname = '_'.join(os.path.basename(trf).split('_')[0:-1])
            #os.system( 'mv {:s} {:s}'.format(msname,obsdir))
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
## get LINC input

def get_linc_inputs( field, obsid ):
    datadir = os.path.join( os.getenv('DATA_DIR'), field, obsid )
    softwaredir = os.getenv('SOFTWAREDIR')
    mslist = glob.glob( os.path.join( datadir, '*.MS' ) )
    singularity_img = os.getenv('LOFAR_SINGULARITY')
    ## download TGSS skymodel
    skymodel = os.path.join( datadir, 'target.skymodel' )
    cmd = "apptainer exec -B {:s},{:s} --no-home {:s} python3 {:s}/LINC/scripts/download_skymodel_target.py --targetname={:s} {:s} {:s}".format( os.getcwd(), softwaredir, singularity_img, softwaredir, field, mslist[0], skymodel )
    if os.system(cmd):
        update_status(field,"TGSS failed")
    #Download IONEX
    ionexpath = datadir
    cal_solutions = os.path.join( datadir, 'LINC-cal_solutions.h5' )
    cmd = "apptainer exec -B {:s},{:s} --no-home {:s} python3 {:s}/LINC/scripts/createRMh5parm.py --ionexpath={:s} --solsetName=target --server='http://ftp.aiub.unibe.ch/CODE/' {:s} {:s}".format( os.getcwd(), softwaredir, singularity_img, softwaredir, datadir, mslist[0], cal_solutions )
    if os.system(cmd):
        update_status(field,"IONEX failed")

##############################
## split directions

def chunk_imagecat( fieldobsid, numdirs=10, catname='image_catalogue.csv', nchunkspertime=2 ):
    basedir = os.getenv('DATA_DIR')
    name = fieldobsid.split('/')[0]
    obsid = fieldobsid.split('/')[1]
    catfile = os.path.join( basedir, '{:s}/{:s}'.format(name,catname) )

    with open( catfile, 'r' ) as f:
        lines = f.readlines()
    header = lines[0]
    lines = lines[1:]

    nchunks = int(np.ceil(len(lines)/numdirs))
    chunk = 1
    for i in np.arange(nchunks):
        j = 10*i
        if i < nchunks - 1:
            mylines = lines[j:j+numdirs]
        else:
            mylines = lines[j:]
        with open( catfile.replace('.csv','_{:s}.csv'.format(str(chunk))), 'w' ) as f:
            f.write(header)
            for myline in mylines:
                f.write(myline)
        chunk = chunk + 1
    cmd = 'sbatch -J split {:s} --array=1-{:s}%{:s} {:s}/lotss-hba-survey/slurm/run_split-directions-toil.sh {:s}'.format(os.getenv('CLUSTER_OPTS'),str(nchunks),str(nchunkspertime),os.getenv('SOFTWAREDIR'),fieldobsid)
    return( cmd )



##############################
## verifying

def get_workflow_obsid(outdir):
    ## get the workflow that was run
    with open(os.path.join(outdir,'job_output.txt'),'r') as f:
        for line in f:
            if 'Resolved' in line:
                break
    tmp = line.split('.cwl')
    workflow = os.path.basename(tmp[0])
    obsid = os.path.basename(outdir)
    return(workflow,obsid)

def check_field(field):
    field_obsids = get_local_obsid(field)
    fieldobsid = '{:s}/{:s}'.format(field,field_obsids[0])
    procdir = os.path.join(str(os.getenv('DATA_DIR')),'processing')
    outdirs = glob.glob(os.path.join(procdir,'{:s}*'.format(fieldobsid)))
    finished = glob.glob(os.path.join(procdir,'{:s}*'.format(fieldobsid),'finished.txt') )
    success = []
    if len(outdirs) == len(finished) and len(outdirs) > 0:
        for outdir in outdirs:
            with open(os.path.join(outdir,'finished.txt'),'r') as f:
                lines = f.readlines()
            if 'SUCCESS: Pipeline finished successfully' in lines[0]:
                success.append(1)
            else:
                success.append(0)
        if sum(success) == len(outdirs):
            ## everything finished successfully
            success = 'Finished'
            workflow, obsid = get_workflow_obsid(outdirs[0])        
        else:
            idx = np.where(np.asarray(success) == 0)
            print('The following pipelines did not finish successfully, please check processing:')
            print( np.asarray(outdirs)[idx] )    
            success = 'Failed'
            workflow, obsid = get_workflow_obsid(outdirs[0])
    elif len(outdirs) > 0:
        ## the number of finished != number of directories - the process is still running
        success = 'Running'
        workflow, obsid = get_workflow_obsid(outdirs[0])
    else:
        ## No outdirs yet, process is still pending
        success = 'Running'
        workflow, obsid = None, None
    return success, workflow, obsid

def cleanup_step(field):
    field_obsids = get_local_obsid(field)
    fieldobsid = field_obsids[0]
    basedir = os.getenv('DATA_DIR')
    procdir = os.path.join(str(os.getenv('DATA_DIR')),'processing')
    field_procdirs = glob.glob( os.path.join(procdir,field, fieldobsid+'*') )
    workflow, tmpid = get_workflow_obsid(field_procdirs[0])
    field_datadir = os.path.join(basedir,field,fieldobsid)
    workflowdir = os.path.join(field_datadir,workflow)
    os.makedirs(workflowdir,exist_ok=True)
    for field_procdir in field_procdirs:
        ## remove logs directory (run was successful)
        os.system('rm -rf {:s}'.format(os.path.join(field_procdir,'logs')))
        ## same for tmp directory
        os.system('rm -rf {:s}'.format(os.path.join(field_procdir,'tmp*')))
        ## and workdir
        os.system('rm -rf {:s}'.format(os.path.join(field_procdir,'workdir')))
        ## move everything else to the data directory and rename MSs
        remaining_files = glob.glob(os.path.join(field_procdir,'*'))
        for ff in remaining_files:           
            dest = os.path.join(workflowdir,os.path.basename(ff).replace('out_',''))
            os.system('mv {:s} {:s}'.format(ff,dest))
        ## remove data from previous step if required
        if workflow in ['setup']:
            os.system('rm -r {:s}'.format(os.path.join(field_datadir, 'setup/*.MS')))
        if workflow in ['HBA_target']:
            os.system('cp {:s} {:s}'.format(os.path.join(workflowdir,'LINC-cal_solutions.h5'),os.path.join(field_datadir,'LINC-target_solutions.h5')))
            ## a results sub-directory in the results directory in 
        if workflow in ['concatenate-flag']:
            os.system('rm -r {:s}'.format(os.path.join(field_datadir,'setup/L*MS')))
        os.system('rmdir {:s}'.format(field_procdir) )

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

