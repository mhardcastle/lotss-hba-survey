id = '516338'

with SurveysDB(readonly=True) as sdb: 
     idd=sdb.db_get('lb_calibrators',id) 
     stage_id = idd['staging_id'] 
sdb.close()

surls = stager_access.get_surls_online(stage_id)    

caldir = os.path.join(str(os.getenv('LINC_DATA_DIR')),str(id))
tarfiles = glob.glob(os.path.join(caldir,'*tar'))
trfs = [ os.path.basename(trf) for trf in tarfiles ]
not_downloaded = [ surl for surl in surls if os.path.basename(surl) not in trfs ]
logfile = '{:s}_gfal.log'.format(id)
os.system('echo Number of files downloaded does not match number staged >> {:s}'.format(logfile))
if 'juelich' in not_downloaded[0]:
    for surl in not_downloaded:
        dest = os.path.join(caldir,os.path.basename(surl))
        os.system('gfal-copy {:s} {:s} > {:s} 2>&1'.format(surl.replace('srm://lofar-srm.fz-juelich.de:8443','gsiftp://lofar-gridftp.fz-juelich.de:2811'),dest,logfile))
tarfiles = glob.glob(os.path.join(caldir,'*tar'))
if len(tarfiles) == len(surls):
    print('Download successful for {:s}'.format(id) )
    update_status(id,'Downloaded',stage_id=0)
    if os.path.exists(logfile):
        os.system('rm {:s}'.format(logfile))
else:
    os.system('echo Attempt to re-download failed >> {:s} 2>&1'.format(logfile))

