def prepare_field(field,basedir,verbose=False):
    cdir = os.getc



def prepare_field(field,processingdir,verbose=False):

    cdir = os.getcwd()
    if not os.path.isdir(processingdir):
        if verbose:
            print('Creating directory',processingdir)
        os.mkdir(processingdir)
    os.chdir(processingdir)

    do_rclone_download(field,processingdir,verbose=verbose)

    success = something

    os.chdir(cdir)

    return success


def do_download(field, basedir='/cosma5/data/durham/dc-mora2/surveys/'):
    update_status(field,'Downloading')
    success=True
    try:
        ## function that downloads, end result is tar files on disk
        prepare_field(field,basedir+'/'+field,verbose=True,operations=['download'])
    except RuntimeError:
        success=False
    if success:
        update_status(field,'Downloaded')
    else:
        update_status(field,'Download failed')



def do_rclone_download(cname,f,verbose=False,macaroon=''):
    '''
    Download required data from field cname into location f
    '''
    ## set up rclone
    try:
        rc=RClone(macaroon,debug=True)
    except RuntimeError:
        print('Macaroon',macaroon,'does not exist!')
        continue ## really?
    rc.get_remote()
    files=rc.get_files( ... )
    tar


    for macaroon, directory in [('maca_sksp_tape_DR2_readonly.conf',''),('maca_sksp_tape_DDF.conf','archive/'),('maca_sksp_tape_DDF_readonly.conf','other/')]:
        try:
            rc=RClone(macaroon,debug=True)
        except RuntimeError:
            print('Macaroon',macaroon,'does not exist!')
            continue
        rc.get_remote()
        files=rc.get_files(directory+cname)
        tarfiles=[fl for fl in files if 'images' in fl or 'uv' in fl]
        if tarfiles:
            d=rc.multicopy(rc.remote+directory+cname,tarfiles,f)
            if d['err'] or d['code']!=0:
                continue
        else:
            continue
        break
        
    else:
        raise RuntimeError('Failed to download from any source')
    tarfiles = glob.glob('*tar')
    untar(f,tarfiles,verbose=verbose)

        # Format of lines is like: srm://srm.grid.sara.nl:8443/pnfs/grid.sara.nl/data/lofar/ops/projects/lt10_010/775619/L775619_SB055_uv.MS_76a2361e.tar
        startdir = os.getcwd()
        os.chdir(outdir)
        for line in srmfile:
                origline = line[:-1]
                line = origline.split('/')
                print(line)
                projectcode = line[-3]
                obsid = line[-2]
                fileid = line[-1]
                if 'psnc' in origline:
                        location = 'Poznan'
                if 'juelich' in origline:
                        location = 'Juelic'
                if 'sara' in origline:
                        location = 'Sara'
                if os.path.exists(fileid.split('_')[0] + '_' + fileid.split('_')[1] + '_'  + fileid.split('_')[2]):
                        print('Already downloaded  ',fileid)
                        continue
                if os.path.exists(fileid.split('_')[0] + '_' + fileid.split('_')[1] + '_'  + fileid.split('_')[2].replace('.MS','_avg.MS')):
                        print('Already downloaded  and averaged',fileid)
                        continue

                print('Downloading %s'%fileid)
                if location != 'Poznan':
                        print('rclone  --multi-thread-streams 1 --config=/project/lotss/Software/prefactor-operations/macaroons/maca_sksp_LTA.conf copy maca_sksp_LTA:/%s/%s/%s  ./'%(projectcode,obsid,fileid))
                        os.system('rclone  --multi-thread-streams 1 --config=/project/lotss/Software/prefactor-operations/macaroons/maca_sksp_LTA.conf copy maca_sksp_LTA:/%s/%s/%s  ./'%(projectcode,obsid,fileid))
                else:
                        print('singularity run -B /etc/grid-security/certificates,/project /cvmfs/atlas.cern.ch/repo/containers/fs/singularity/x86_64-centos7 gfal-copy gsiftp://gridftp.lofar.psnc.pl:2811/lofar/ops/projects/%s/%s/%s ./'%(projectcode,obsid,fileid))
                        os.system('singularity run -B /etc/grid-security/certificates,/project /cvmfs/atlas.cern.ch/repo/containers/fs/singularity/x86_64-centos7 gfal-copy gsiftp://gridftp.lofar.psnc.pl:2811/lofar/ops/projects/%s/%s/%s ./'%(projectcode,obsid,fileid))


                print('Unpacking %s'%fileid)
                os.system('tar -xf %s'%fileid)
                if compress:
                        msfilename = fileid.split('_')[0] + '_' + fileid.split('_')[1] + '_'  + fileid.split('_')[2]
                        compress_average(msfilename)
                os.system('rm %s'%fileid)
        os.chdir(startdir)
        return
