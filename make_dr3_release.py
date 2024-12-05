#!/usr/bin/env python

from __future__ import print_function
from builtins import range
import os
from auxcodes import separator,warn
from surveys_db import SurveysDB
from shutil import copy2
from subprocess import call
from time import sleep
from datetime import datetime
import glob

def link(name,id,lroot,anchor,wdir):
    if os.path.isfile(wdir+id+'/'+name):
        return '<a href="%s%s">%s</a>' % (lroot,name,anchor)
    else:
        return '&mdash;'
    
if __name__=='__main__':
    skip_construct=False
    while True:
        print(datetime.now())
        print()

        os.system('chmod +r /beegfs/car/mjh/torque/*')
        
        # make status plot
        separator('Making plots')
        os.system('plot_db_projection.py /home/mjh/lofar-surveys/static/Tier1-dbstatus.png')
        os.system('plot_mosaics.py /home/mjh/lofar-surveys/static/mosaic.png')

        # make JSON file for HIPS
        separator('Make JSON file')
        os.chdir('/home/mjh/lofar-surveys/static/lotss_aladin')
        os.system('python survey_status_mysql.py')

        '''
        if not skip_construct:
            # sync mosaics directory
            separator('Mosaic sync')
            os.chdir(workdir+'/mosaics')
            for d in ['RA13h_field','RA0h_field']:
                s="rsync --progress --timeout=60 -avz --exclude 'astroblank-*' --exclude '*.out' --exclude '*.py' --exclude 'background' --exclude '*~' --exclude 'old' --exclude '*.sh' --exclude 'low-mosaic-weights.fits' --exclude 'mosaic.fits' --exclude 'wavelet' --exclude 'model' --exclude 'residual' --exclude 'mosaic.pybdsmmask.fits' --exclude 'mosaic-weights.fits' --exclude 'reproject-*.fits' --exclude 'weight-*.fits' --exclude 'low-reproject-*.fits' --exclude 'low-weight-*.fits' --exclude 'low-mosaic.fits' %s@ssh.strw.leidenuniv.nl:/disks/paradata/shimwell/LoTSS-DR2/mosaics/%s/ ." % (os.environ['DDF_PIPELINE_LEIDENUSER'],d)
                do_rsync(s)
        '''

        # now go through all archived and completed fields and make sure they're in the DR2 directory

        with SurveysDB() as sdb:
            sdb.cur.execute('select * from fields left join quality on fields.id=quality.id where dr2 or status="Archived" or status="Complete" or status="Verified" order by ra')
            result=sdb.cur.fetchall()

        print('There are',len(result),'complete datasets')

        if not skip_construct:
            separator('Preparing release directory')
            releasefiles=['image_full_high_stokesV*dirty.fits','image_full_high_stokesV*dirty.corr.fits','image_full_high_stokesV.SmoothNorm.fits','image_full_low_m.int.restored.fits','image_full_low_m.app.restored.fits','image_full_ampphase_di_m.NS.tessel.reg','image_full_ampphase_di_m.NS.app.restored.fits','image_full_ampphase_di_m.NS.int.restored.fits']

            for r in result:
                id=r['id']
                if not os.path.isdir(id):
                    warn('Directory %s does not exist, making it' % id)
                    os.mkdir(id)
                #if r['dr2']:
                #    continue # skip dr2
                if r['proprietary_date'] is None:
                    workdir='/data/lofar/DR3/fields'
                else:
                    workdir='/data/lofar/fields_proprietary'
                print('Doing',id,r['clustername'],r['location'],r['status'])
                if not os.path.isdir(id):
                    warn('Directory %s does not exist, making it' % id)
                    os.mkdir(id)
                os.chdir(workdir)
                tdir=workdir+'/'+id
                if r['clustername']=='Herts' and r['location']!="" and (r['status']=='Verified' or r['status']=='Complete'):
                    location=r['location']
                    resolved_release=[]
                    for f in releasefiles:
                        if '*' in f:
                            try:
                                resolved_release+=[os.path.basename(g) for g in glob.glob(location+'/'+f)]
                            except TypeError:
                                print('Issue with file',f)
                        else:
                            resolved_release.append(f)                       

                    if location:
                        for f in resolved_release:
                            source=location+'/'+f
                            if not os.path.isfile(tdir+'/'+f) or (os.path.isfile(source)  and os.path.getmtime(source)>os.path.getmtime(tdir+'/'+f)):
                                if os.path.isfile(source):
                                    print('Need to copy',f,'to',tdir)
                                    copy2(source,tdir)
                                else:
                                    warn('Source file %s does not exist' % source)
                        os.system('chmod og+r %s/*' % id)
                else:
                    # get from archive if necessary
                    if r['status']=='Verified':
                        # it's verified but not local: it should be in the archive
                        failcount=0
                        for f in releasefiles:
                            if '*' in f:
                                continue
                                #g=glob.glob(tdir+'/'+f)
                                #if len(g)==0:
                                #    print('Cannot find files',(tdir+'/'+f))
                                #    failcount+=1
                            else:
                                if not os.path.isfile(tdir+'/'+f):
                                    if 'high_stokesV' in f: continue
                                    print('Need to download',id+'/'+f,'from archive')
                                    failcount+=1
                        if failcount>0 and r['dr3']:
                            os.system('get_images.py '+id)


        separator('Write web pages')

        for page in ['dr3']:
            outfile=open('/home/mjh/lofar-surveys/templates/'+page+'-mosaics.html','w')
            workdir='/data/lofar/DR3'
            for r in result:
                id=r['id']
                fwdir=workdir+'/mosaics/'+id
                if os.path.isdir(fwdir) and os.path.isfile(fwdir+'/mosaic-blanked.fits'):
                    if page=='dr3':
                        root='downloads'
                    else:
                        root='public'
                    root+='/DR3/mosaics/'+id+'/'
                    f=link('mosaic-blanked.fits',id,root,'Download',workdir+'/mosaics/')
                    rms=link('mosaic-blanked--final.rms.fits',id,root,'Download',workdir+'/mosaics/')
                    resid=link('mosaic-blanked--final.resid.fits',id,root,'Download',workdir+'/mosaics/')
                    weights=link('mosaic-weights.fits',id,root,'Download',workdir+'/mosaics/')
                    mask=link('mosaic-blanked--final.mask.fits',id,root,'Download',workdir+'/mosaics/')
                    low=link('low-mosaic-blanked.fits',id,root,'Download',workdir+'/mosaics/')
                    lowweight=link('low-mosaic-weights.fits',id,root,'Download',workdir+'/mosaics/')
                    catalogue=link('mosaic-blanked--final.srl.fits',id,root,'Download',workdir+'/mosaics/')
                    #image=root+'mosaic-blanked.png'
                    #headers=root+'fits_headers.tar'
                    outfile.write('<tr><td>%s</td><td>%.3f</td><td>%.3f</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>\n' % (id,r['ra'],r['decl'],f,rms,resid,weights,mask,low,lowweight,catalogue))
            outfile.close()

        outfile=open('/home/mjh/lofar-surveys/templates/dr3-fields.html','w')

        for r in result:
            id=r['id']
            if r['dr2']:
                lroot='downloads/DR2/fields/'+id+'/'
                workdir='/data/lofar/DR2'
            else:
                lroot='downloads/DR3/fields/'+id+'/'
                workdir='/data/lofar/DR3'
            if os.path.isdir(workdir+'/fields/'+id):
                fint=link('image_full_ampphase_di_m.NS.int.restored.fits',id,lroot,'True',workdir+'/fields/')
                fapp=link('image_full_ampphase_di_m.NS.app.restored.fits',id,lroot,'App',workdir+'/fields/')
                lowint=link('image_full_low_m.int.restored.fits',id,lroot,'True',workdir+'/fields/')
                lowapp=link('image_full_low_m.app.restored.fits',id,lroot,'App',workdir+'/fields/')
                #band=[]
                #for i in range(3):
                #    band.append(link('image_full_ampphase_di_m.NS_Band%i_shift.int.facetRestored.fits' % i,id,lroot,'%i' %i, workdir+'/fields/'))
                stokesv=link('image_full_low_stokesV.dirty.fits',id,lroot,'Download',workdir+'/fields/')
                if stokesv.startswith('&'):
                    stokesv=link('image_full_high_stokesV.dirty.corr.fits',id,lroot,'Download',workdir+'/fields/')
                    
                #stokesqu=link('image_full_low_QU.cube.dirty.corr.fits.fz',id,lroot,'Low true',workdir+'/fields/')
                #stokesquvlow=link('image_full_vlow_QU.cube.dirty.corr.fits.fz',id,lroot,'Vlow true',workdir+'/fields/')
                #stokesqu_app=link('image_full_low_QU.cube.dirty.fits.fz',id,lroot,'Low app',workdir+'/fields/')
                #stokesquvlow_app=link('image_full_vlow_QU.cube.dirty.fits.fz',id,lroot,'Vlow app',workdir+'/fields/')
                if r['nvss_scale'] is None:
                    scale='&mdash;'
                else:
                    scale="%.3f" % (5.9124/r['nvss_scale'])
                outfile.write('<tr><td>%s</td><td>%.3f</td><td>%.3f</td><td>%s</td><td>%s</td><td>%s, %s</td><td>%s, %s</td><td>%s</td></tr>\n' % (id,r['ra'],r['decl'],r['end_date'],scale,fint,fapp,lowint,lowapp,stokesv)) #,stokesqu,stokesquvlow,stokesqu_app,stokesquvlow_app))

        outfile.close()

        separator('Publications list')
        os.system('python /home/mjh/python/ads_library.py')

        separator('Quality pipeline')
        os.system('queue_quality.py')
        separator('Sleeping')

        sleep(7200)
