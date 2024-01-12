# Check that a calibrator solution exists for a given observation
# and do other operations on the LOFAR VLBI calibrators

from rclone import RClone
from surveys_db import SurveysDB

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
            calibrators.append(r['id'])
    return(set(calibrators))

def download_field_calibrators(field,wd,verbose=False):
    # download the LOFAR-VLBI calibrator solutions for the field into the specified parent working directory. return dictionary of downloaded cals
    rd={}
    with SurveysDB(readonly=True) as sdb:
        sdb.execute('select * from observations where field=%s',(field,))
        results=sdb.cur.fetchall()
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
    return rd
        

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
    find_calibrators(619688)
    
    
