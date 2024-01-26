# Check that a calibrator solution exists for a given observation
# and do other operations on the LOFAR VLBI calibrators

from __future__ import print_function
from rclone import RClone,splitlines
from surveys_db import SurveysDB
import os
import subprocess

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

if __name__=='__main__':
    #calid=783901
    #result=check_calibrator(calid)
    #print('checking %i: %s' % (calid,result))
    #if result:
    #    download_calibrator(calid,'/beegfs/car/mjh/lb')
    #print(find_calibrators(619688))
    d=download_field_calibrators('P206+37','/beegfs/car/mjh/lb',verbose=True)
    unpack_calibrator_sols('/beegfs/car/mjh/lb/',d,verbose=True)
    
