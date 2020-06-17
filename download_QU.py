from surveys_db import SurveysDB

with SurveysDB(readonly=True) as sdb:
    sdb.cur.execute('select * from fields where decl>30 and decl<70 and ra>120 and ra<250 and status="Archived"')
    res=sdb.cur.fetchall()

for r in res:
    id=r['id']
    print "wget --user surveys --password 150megahertz https://lofar-surveys.org/downloads/DR2/fields/%s/image_full_vlow_QU.cube.dirty.fits.fz -O %s_vlow_QU.cube.dirty.fz" % (id,id)
    print "wget --user surveys --password 150megahertz https://lofar-surveys.org/downloads/DR2/fields/%s/image_full_vlow_QU.cube.dirty.corr.fits.fz -O %s_vlow_QU.cube.dirty.corr.fz" % (id,id)
    
    
