# Check that a calibrator solution exists for a given observation
# and do other operations on the LOFAR VLBI calibrators

from __future__ import print_function
from __future__ import division
from rclone import RClone,splitlines
from surveys_db import SurveysDB
import os
import subprocess
import glob
import fnmatch
import datetime
from losoto.h5parm import h5parm
import numpy as np
from reprocessing_utils import do_sdr_and_rclone_download, do_rclone_download

#################################
'''
export MACAROON_DIR=/home/azimuth/macaroons/
'''

def get_linc( obsid, caldir ):
    ## find the target solutions -- based on https://github.com/mhardcastle/ddf-pipeline/blob/master/scripts/download_field.py
    macaroons = ['maca_sksp_tape_spiderlinc.conf','maca_sksp_tape_spiderpref3.conf','maca_sksp_distrib_Pref3.conf']
    rclone_works = True
    obsname = 'L'+str(obsid)
    success = False
    thismac = None
    for macaroon in macaroons:
        macname = os.path.join(os.getenv('MACAROON_DIR'),macaroon)
        try:
            rc = RClone(macname,debug=True)
        except RuntimeError as e:
            print('rclone setup failed, probably RCLONE_CONFIG_DIR not set:',e)
            rclone_works=False
                
        if rclone_works:
            try:
                remote_obs = rc.get_dirs()
            except OSError as e:
                print('rclone command failed, probably rclone not installed or RCLONE_COMMAND not set:',e)
                rclone_works=False
        
        if rclone_works and obsname in remote_obs:
            print('Data available in rclone repository, downloading solutions!')
            thismac = macname
            d = rc.execute(['-P','ls',rc.remote + obsname])
            filelist = d['out']
            tarfiles = [ line.split(' ')[-1] for line in filelist if 'tar' in line ]
            tarfile = [ tf for tf in tarfiles if 'cal' in tf and 'ms' not in tf ][0]

            d = rc.execute(['-P','copy',rc.remote + os.path.join(obsname,tarfile)]+[caldir]) 
            if d['err'] or d['code']!=0:
                print('Rclone failed to download solutions')
            else:
                untar_file(os.path.join(caldir,tarfile),caldir+'/tmp','*h5',os.path.join(caldir,'LINC-target_solutions.h5'),verbose=False)
                d = rc.execute(['-P','copy',rc.remote + os.path.join(obsname,'inspection.tar')]+[caldir]) 
                if d['err'] or d['code']!=0:
                    print('Rclone failed to download inspection plots')
                d = rc.execute(['-P','copy',rc.remote + os.path.join(obsname,'logs.tar')]+[caldir]) 
                if d['err'] or d['code']!=0:
                    print('Rclone failed to download logs')
                ## check that solutions are ok (tim scripts)
                sols = glob.glob(os.path.join(caldir,'LINC-target_solutions.h5'))[0]
                success = check_solutions(sols)
    return(success,thismac)

def ddfpipeline_timecheck(name,soldir):
    do_sdr_and_rclone_download(name,soldir,verbose=False,Mode="Misc",operations=['download'])
    untar_file(os.path.join(soldir,'misc.tar'),os.path.join(soldir,'tmp'),'*crossmatch-results-2.npy',os.path.join(soldir,'timetest-crossmatch-results-2.npy'))
    ddftime = os.path.getmtime(os.path.join(soldir,'timetest-crossmatch-results-2.npy'))
    os.system('rm {:s}'.format(os.path.join(soldir,'timetest-crossmatch-results-2.npy')))
    return(ddftime)

def get_linc_for_ddfpipeline(macname,caldir):
    obsname = 'L' + os.path.basename(caldir)
    try:
        rc = RClone(macname,debug=True)
        rclone_works=True
    except RuntimeError as e:
        print('rclone setup failed, probably RCLONE_CONFIG_DIR not set:',e)
        rclone_works=False                    
    if rclone_works:
        try:
            remote_obs = rc.get_dirs()
        except OSError as e:
            print('rclone command failed, probably rclone not installed or RCLONE_COMMAND not set:',e)
            rclone_works=False
    if rclone_works:
        d = rc.execute(['-P','ls',rc.remote + obsname])
        filelist = d['out']
        tarfiles = [ line.split(' ')[-1] for line in filelist if 'tar' in line ]
        msfiles = [ tf for tf in tarfiles if 'ms' in tf ]
        ddfpipelinedir = os.path.join(caldir,'HBA_target/results')
        os.makedirs(ddfpipelinedir)
        for msfile in msfiles:
            d = rc.execute(['-P','copy',rc.remote + os.path.join(obsname,msfile)]+[ddfpipelinedir])
        if d['err'] or d['code']!=0:
            print('Rclone failed to download solutions')
        ## untar them
        untar_files = glob.glob(os.path.join(ddfpipelinedir,'*tar'))
        for trf in untar_files:
            untar_ms(trf,ddfpipelinedir)

def download_ddfpipeline_solutions(name,soldir,ddflight=False):
    if not os.path.isdir(soldir):
        os.makedirs(soldir)
    do_sdr_and_rclone_download(name,soldir,verbose=False,Mode="Imaging",operations=['download'])
    image_tar = os.path.join(soldir,'images.tar') 
    uv_tar = os.path.join(soldir,'uv.tar')
    misc_tar = os.path.join(soldir,'misc.tar')
    untar_files = ['image_full_ampphase_di_m.NS.app.restored.fits','image_full_ampphase_di_m.NS.mask01.fits','image_full_ampphase_di_m.NS.DicoModel']
    for utf in untar_files:
        untar_file(image_tar,os.path.join(soldir,'tmp'),utf,os.path.join(soldir,utf))
    if ddflight:
        untar_files = ['image_dirin_SSD_m.npy.ClusterCat.npy']
    else:
        untar_files = ['image_dirin_SSD_m.npy.ClusterCat.npy','SOLSDIR']
    for utf in untar_files:
        untar_file(uv_tar,os.path.join(soldir,'tmp'),utf,os.path.join(soldir,utf))
    if not ddflight:
        untar_files = ['logs/*DIS2*log','L*frequencies.txt']
        for utf in untar_files:
            untar_file(misc_tar,soldir,utf,os.path.join(soldir,utf))
            freq_check = glob.glob(os.path.join(soldir,'L*frequencies.txt'))
        if len(freq_check) > 0:
            success = True
        else:
            success = False
    else:
        success = True
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
    return(success)

def find_calibrators(obsid):
    # Find all the calibrators appropriate for a given obsid by
    # 1. looking at the observations table
    # 2. searching the lb_calibrators table by time to find others
    calibrators=[]
    with SurveysDB(readonly=True) as sdb:
        r=sdb.db_get('observations',obsid)

        if r is None:
            raise RuntimeError('Cannot find obsid %s' % obsid)
        calibrators.append(int(r['calibrator_id']))

        # now check dates
        sdb.execute('select * from lb_calibrators where abs(timestampdiff(second,obs_date,%s))<43200',(r['date'],))
        results=sdb.cur.fetchall()
        for r in results:
            calibrators.append(int(r['id']))
    return(set(calibrators))

def download_field_calibrators(field,wd,verbose=False):
    # download the LOFAR-VLBI calibrator solutions for the field into the specified parent working directory. return dictionary of downloaded cals
    rd={}
    with SurveysDB(readonly=True) as sdb:
        sdb.execute('select * from observations where field=%s',(field,))
        results=sdb.cur.fetchall()
    if not results:
        raise RuntimeError('No observations found for field '+field)
    for r in results:
        obsid=r['id']
        if verbose: print('Doing obsid',obsid)
        rd[obsid]=[]
        calibrators=find_calibrators(str(r['id']))
        for calid in calibrators:
            if verbose: print('     Checking calibrator',calid)
            if check_calibrator(calid):
                download_calibrator(calid,wd)
                rd[obsid].append(calid)
            elif verbose: print('     No processed calibrator found!')
    return rd


def untar_file(tarfile,tmpdir,searchfile,destfile,verbose=False):
    if not os.path.isdir(tmpdir):
        os.mkdir(tmpdir)
    command=['tar','tf',tarfile]
    proc=subprocess.Popen(command,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    (out, err) = proc.communicate()
    files=splitlines(out)
    for f in files:
        if fnmatch.fnmatch(os.path.basename(f),searchfile):
            fullpath=f
            break
    else:
        raise RuntimeError('Cannot find file %s in tarfile %s' % (searchfile,tarfile))
    depth=fullpath.count('/')
    command='tar --strip-components=%i -C %s -xf %s %s' % (depth,tmpdir,tarfile,fullpath)
    if verbose: print('running',command)
    os.system(command)
    if verbose: print('renaming',tmpdir+'/'+os.path.basename(fullpath),'as',destfile)
    os.rename(tmpdir+'/'+os.path.basename(fullpath),destfile)
    os.rmdir(tmpdir)
    
def unpack_calibrator_sols(wd,rd,verbose=False):
    # Unpack the calibrator tar files into the obsid directories and
    # name them suitably.  wd is the working directory for the
    # field. rd is the dictionary returned by
    # download_field_calibrators, i.e. a list of calibrator files for
    # each obsid
    sollist = []
    for obsid in rd:
        if verbose: print('Doing obsid',obsid)
        for cal in rd[obsid]:
            tarfile=os.path.join(wd, '%i.tgz' % cal )
            if not os.path.isfile(tarfile):
                raise RuntimeError('Cannot find the calibrator tar file!')
            untar_file(tarfile,wd+'/tmp','cal*solutions.h5',os.path.join(wd,'%i_solutions.h5' % cal),verbose=verbose)
            sollist.append(os.path.join(wd,'%i_solutions.h5' % cal))
    return(sollist)

def check_calibrator(calid):
    macaroon_dir = os.getenv('MACAROON_DIR')        
    maca = glob.glob(os.path.join(macaroon_dir,'*lofarvlbi.conf'))[0]
    rc=RClone(maca)
    files=rc.get_files('disk/surveys/'+str(calid)+'.tgz')
    return len(files)>0

def download_calibrator(calid,dest):
    macaroon_dir = os.getenv('MACAROON_DIR')        
    maca = glob.glob(os.path.join(macaroon_dir,'*lofarvlbi.conf'))[0]
    rc=RClone(maca)
    rc.get_remote()
    rc.copy(rc.remote+'disk/surveys/'+str(calid)+'.tgz',dest)

def check_cal_clock(calh5parm,verbose=False):
    data=h5parm(calh5parm,'r')
    cal = data.getSolset('calibrator')
    soltabs = list(cal.getSoltabNames())
    data.close()
    if verbose: print(soltabs)
    if 'clock' not in soltabs:
        if verbose: print('Calibrator is bad')
        return False
    if 'bandpass' not in soltabs:
        if verbose: print('Calibrator is bad')
        return False
    if verbose: print('Calibrator good - returning')
    return True

## clock and bandpass are the crucial ones here
## so probably the above code can be merged with something that does the important stuff?

def isint(ant):
    return not(ant.startswith('CS') or ant.startswith('RS'))

def check_int_stations(calh5parm,verbose=False,n_req=9):
    # Check number of int stations and flagging fraction for clock and bandpass
    if verbose: print('Opening',calh5parm)
    data=h5parm(calh5parm,'r')
    cal = data.getSolset('calibrator')
    soltabs = list(cal.getSoltabNames())
    if verbose: print('Solution tables are:',soltabs)
    d={}
    if 'clock' not in soltabs:
        d['err']='no_clock'
    elif 'bandpass' not in soltabs:
        d['err']='no_bandpass'
    else:
        clock=cal.getSoltab('clock')
        v,a=clock.getValues()
        good=[ant for ant in a['ant'] if isint(ant)]
        count=len(good)
        d['n_int']=count
        if count==0:
            d['err']='no_international'
        elif count<n_req:
            d['err']='too_few_international'
        else:
            assert(v.shape[1]==len(a['ant']))
            nclock=v.shape[0]
            # Now check the flag fractions
            if verbose: print('Checking clock flag fractions')
            nffc=0
            for i,ant in enumerate(a['ant']):
                flagf=np.sum(np.isnan(v[:,i]))/nclock
                if isint(ant):
                    if verbose: print("%-12s %7.2f%%" % (ant,100*flagf))
                    if flagf>0.5:
                        nffc+=1
                        if ant in good: good.remove(ant)
            d['flagged_clock']=nffc

            bp=cal.getSoltab('bandpass')
            v,a=bp.getValues()
            assert(v.shape[2]==len(a['ant']))
            nbp=v.shape[0]*v.shape[1]*v.shape[3]
            if verbose: print('Checking bandpass flag fractions')
            nffb=0
            for i,ant in enumerate(a['ant']):
                flagf=np.sum(np.isnan(v[:,:,i,:]))/nbp
                if isint(ant):
                    if verbose: print("%-12s %7.2f%%" % (ant,100*flagf))
                    if flagf>0.5:
                        nffb+=1
                        if ant in good: good.remove(ant)
            d['flagged_bp']=nffb
            d['n_good']=len(good)
            if len(good)<n_req:
                d['err']='too_few_good'
    data.close()
    return d
    
def check_cal_flag(calh5parm):
    print('Running losoto to check cal flagging')
    sing_img = os.getenv('LOFAR_SINGULARITY')
    flaginfo = calh5parm.replace('.h5','.info')
    if not os.path.isfile(flaginfo):
        cmd = 'singularity exec -B {:s} {:s} losoto -iv {:s} > {:s}'.format(os.getcwd(),sing_img,calh5parm,flaginfo)
        os.system(cmd)
    print('Checking outputfile for flaggging')
    with open(flaginfo,'r') as f:
        lines = f.readlines()

    flagdict={}
    for line in lines:
        line = line[:-1]
        print(line)
        line = line.split(' ')
        while '' in line:
            line.remove('')
        if len(line) < 3:
            continue
        if 'Solution' in line[0] and 'table' in line[1]:
            flagtype = line[2].replace("'",'').replace("'",'')
        if 'Flagged' in line[0] and 'data' in line[1]:
            flagdict[flagtype] = float(line[2].replace('%',''))
    print('Flag dict',flagdict)
    for element in flagdict:
        print(element,flagdict[element],element=='bandpass')
        if element == 'bandpass':
            if flagdict[element] > 10.0:
                print('badbandpass')
                return('badflag')
        if element == 'clock':
            if flagdict[element] > 10.0:
                print('badclock')
                return('badflag')
        if element == 'faraday':
            if flagdict[element] > 10.0:
                print('badfaraday')
                return('badflag')
        if element == 'polalign':
            if flagdict[element] > 10.0:
                print('badpolalign')
                return('badflag')
    ## number of flagged intl stations
    ## average flagging for intl stations
    return(flagdict)

def check_solutions(calh5parm,n_req=9,verbose=False):
    stations = check_int_stations(calh5parm,n_req=n_req)
    if stations['n_good'] > n_req:
        ## good, check flagging level
        flagdict = check_cal_flag(calh5parm)
        ## check that clock and bandpass are solutions
        if 'clock' not in flagdict.keys():
            if verbose: print('Calibrator is bad')
            return False
        if 'bandpass' not in flagdict.keys():
            if verbose: print('Calibrator is bad')
            return False
        if verbose: print('Calibrator good - returning')
        ## check flagging level
        for fkey in flagdict.keys():
            if flagdict[fkey] > 10:
                print('Excessive flagging detected in {:s}!!'.format(fkey))
                return False
    else:
        if verbose: print('Not enough int stations')
        return False
    return True

def compare_solutions(sollist):
    stn_compare = []
    sols_good = []
    for solfile in sollist:
        stations = check_int_stations(solfile)
        stn_compare.append(stations['n_good'])
        solcheck = check_solutions(solfile)
        sols_good.append(solcheck)
    max_stns = np.where(stn_compare == np.max(stn_compare))[0]
    if len(max_stns) == 1 and np.asarray(sols_good)[max_stns]:
        best_sols = np.asarray(sollist)[max_stns][0]
    else:
        fr = []
        bp = []
        for solfile in sollist:
            flaginfo = check_cal_flag(solfile)
            fr.append(flaginfo['faraday'])
            bp.append(flaginfo['bandpass'])
        low_fr = np.where(fr == np.min(fr))[0]
        if len(low_fr) == 1 and np.asarray(sols_good)[low_fr]:
            best_sols = np.asarray(sollist)[low_fr][0]
        else:
            low_bp = np.where(bp == np.min(bp))[0]
            if len(low_bp) == 1:
                best_sols = np.asarray(sollist)[low_bp][0]
            else:
                best_sols = sollist[0]
    return([best_sols])


def update_db_stats(wd):
    # One-off function to take a directory containing *_solutions.h5,
    # run the quality checker and update the database for all
    # solutions found.
    os.chdir(wd)
    g=glob.glob('*_solutions.h5')
    with SurveysDB() as sdb:
        for f in g:
            calid=f.split('_')[0]
            idd=sdb.db_get('lb_calibrators',calid)
            mtime=os.path.getmtime(f)
            if 'end_date' not in idd or idd['end_date'] is None:
                idd['end_date']=str(datetime.datetime.fromtimestamp(os.path.getmtime(f)))
            d=check_int_stations(f)
            for k in d:
                idd[k]=d[k]
            sdb.db_set('lb_calibrators',idd)
            print(idd)
            

    
if __name__=='__main__':
    #calid=783901
    #result=check_calibrator(calid)
    #print('checking %i: %s' % (calid,result))
    #if result:
    #    download_calibrator(calid,'/beegfs/car/mjh/lb')
    #print(find_calibrators(619688))
    d=download_field_calibrators('P206+37','/beegfs/car/mjh/lb',verbose=True)
    unpack_calibrator_sols('/beegfs/car/mjh/lb/',d,verbose=True)
    
