# ingest file from Francesco

from astropy.table import Table
from surveys_db import SurveysDB

t=Table.read('/beegfs/lofar/mjh/lba/allsky-grid.fits')

with SurveysDB(survey='lba') as sdb:
    for r in t:
        f=sdb.create_field(r['name'])
        f['ra']=r['radeg']
        f['decl']=r['decdeg']
        f['gal_l']=r['GAL_LONG']
        f['gal_b']=r['GAL_LAT']
        f['status']='Not started'
        f['lotss_field']=1
        sdb.set_field(f)
        
        
    
