from surveys_db import SurveysDB
from astropy.coordinates import SkyCoord, EarthLocation, AltAz
import astropy.units as u
from astropy.time import Time
import numpy as np

obs=EarthLocation(lon=(6+(52/60.0))*u.deg, lat=53*u.deg, height=0*u.m)

with SurveysDB(readonly=True) as sdb:
    sdb.cur.execute('select id,ra,decl from fields where status="Archived"')
    fields=sdb.cur.fetchall()

for r in fields:
    print 'Doing',r['id']
    target=SkyCoord(r['ra'],r['decl'],unit='deg')
    with SurveysDB(readonly=True) as sdb:
        sdb.cur.execute('select * from observations where field="%s"' % r['id'])
        observations=sdb.cur.fetchall()
    for o in observations:
        if o['elevation_mean'] is not None:
            continue
        try:
            time=Time(o['date'])
        except ValueError:
            time=None
        if time is None:
            continue
        dt=np.linspace(0,o['integration'],100)*u.hour
        times=time+dt
        frame=AltAz(obstime=times,location=obs)
        taltaz = target.transform_to(frame)
        elevation=taltaz.alt.value
        o['elevation_mean']=np.mean(elevation)
        o['elevation_max']=np.max(elevation)
        o['elevation_min']=np.min(elevation)
        print o
        with SurveysDB() as sdb:
            sdb.set_observation(o)


        
