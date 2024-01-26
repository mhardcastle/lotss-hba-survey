from __future__ import print_function
from __future__ import division
import os
from losoto.h5parm import h5parm
import numpy as np

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

def check_int_stations(calh5parm,verbose=False):
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
        elif count<11:
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
            if len(good)<11:
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

if __name__=='__main__':
    import sys
    print(check_int_stations(sys.argv[1],verbose=True))
    
