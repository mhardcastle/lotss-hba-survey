from surveys_db import SurveysDB
from astropy.coordinates import SkyCoord
import astropy.units as u

def add_galcoords(sdb,result):
    ra=[]
    dec=[]
    for r in result:
        ra.append(r['ra'])
        dec.append(r['decl'])

    sc=SkyCoord(ra=ra,dec=dec,unit=(u.deg,u.deg),frame='icrs')
    ls=sc.galactic.l.value
    bs=sc.galactic.b.value
    
    for i,r in enumerate(result):
        r['gal_l']=ls[i]
        r['gal_b']=bs[i]
        sdb.set_field(r)
    

if __name__=='__main__':

        
    with SurveysDB() as sdb:
        sdb.cur.execute('select * from fields where gal_b is NULL order by ra')
        result=sdb.cur.fetchall()

        add_galcoords(sdb,result)
        

