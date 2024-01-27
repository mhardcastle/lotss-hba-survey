# Check that a calibrator solution exists for a given observation
# and do other operations on the LOFAR VLBI calibrators

from __future__ import print_function
from __future__ import division
from rclone import RClone,splitlines
from surveys_db import SurveysDB
import os
import subprocess
import glob
import datetime
from losoto.h5parm import h5parm
import numpy as np

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
            calibrators=find_calibrators(r['id'])
            for calid in calibrators:
                if verbose: print('     Checking calibrator',calid)
                if check_calibrator(calid):
                    dest=wd+'/%i/' % obsid
                    download_calibrator(calid,dest)
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
        if os.path.basename(f)==searchfile:
            fullpath=f
            break
    else:
        raise RuntimeError('Cannot find file %s in tarfile %s' % (searchfile,tarfile))
    depth=fullpath.count('/')
    command='tar --strip-components=%i -C %s -xf %s %s' % (depth,tmpdir,tarfile,fullpath)
    if verbose: print('running',command)
    os.system(command)
    if verbose: print('renaming',tmpdir+'/'+searchfile,'as',destfile)
    os.rename(tmpdir+'/'+searchfile,destfile)
    os.rmdir(tmpdir)
    
def unpack_calibrator_sols(wd,rd,verbose=False):
    # Unpack the calibrator tar files into the obsid directories and
    # name them suitably.  wd is the working directory for the
    # field. rd is the dictionary returned by
    # download_field_calibrators, i.e. a list of calibrator files for
    # each obsid
    for obsid in rd:
        if verbose: print('Doing obsid',obsid)
        for cal in rd[obsid]:
            dest=wd+'/%i/' % obsid
            tarfile=dest+'%i.tgz' % cal
            if not os.path.isfile(tarfile):
                raise RuntimeError('Cannot find the calibrator tar file!')
            untar_file(tarfile,wd+'/tmp','cal_solutions.h5',dest+'/%i_solutions.h5' % cal,verbose=verbose)

def check_calibrator(calid):
    rc=RClone('*lofarvlbi.conf')
    files=rc.get_files('disk/surveys/'+str(calid)+'.tgz')
    return len(files)>0

def download_calibrator(calid,dest):
    rc=RClone('*lofarvlbi.conf')
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


    return

def update_db_stats(wd):
    # One-off function to take a directory containing *_solutions.h5,
    # run the quality checker and update the database for all
    # solutions found.
    
    from surveys_db import SurveysDB
    import glob
    import datetime
    
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
    
