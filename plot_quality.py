from __future__ import print_function
from surveys_db import SurveysDB
import matplotlib.pyplot as plt
from astropy.table import Table
import numpy as np

# pack up the quality results into an astropy table for ease of plotting columns

with SurveysDB() as sdb:
    sdb.cur.execute('select fields.id,fields.ra,fields.decl,quality.*,avg(observations.elevation_mean) as elevation_mean,avg(integration) as avg_integration,sum(integration) as tot_integration from quality left join fields on fields.id=quality.id left join observations on observations.field=quality.id where fields.status="Verified" and dr3 group by fields.id order by fields.id')
    results=sdb.cur.fetchall()

td={}
for key in results[0]:
    if '.' in key:
        continue
    print(key)
    vl=[]
    for r in results:
        vl.append(r[key])
    vl=[np.nan if f is None else f for f in vl]
    td[key]=vl


t=Table(td)
t['scaled_rms']=t['rms']/(t['nvss_scale']/5.91)
t['scaled_vs_predicted']=t['scaled_rms']/(100.0/np.cos((t['decl']-53.0)*np.pi/180)**2.0)
t.write('quality_table.fits',overwrite=True)

print(len(t))

#t=t[~(t['scale']==None)]
t=t[~np.isnan(t['nvss_scale'])]

#t=t[~(t['tgss_scale']==None)]


plt.figure(figsize=(10,8))
#plt.scatter(t['sc_index'],t['scale'])
'''
plt.scatter(t['tgss_scale'],t['nvss_scale']/4.91,alpha=0.5)
plt.xlabel('TGSS factor')
plt.ylabel('NVSS factor ($\\alpha = 0.7$)')
plt.plot([0,1.8],[0,1.8],color='orange')
print np.median(t['tgss_scale']),np.median(t['nvss_scale']/4.91)
'''
'''
plt.scatter(t['decl'],t['tgss_scale'],alpha=0.5,label='TGSS')
plt.scatter(t['decl'],t['nvss_scale']/4.91,alpha=0.5,label='NVSS ($\\alpha=0.7$)')
plt.xlabel('Declination (deg)')
plt.ylabel('Factor')
plt.legend(loc=0)
'''
'''
plt.scatter(t['nvss_scale']/4.91,t['scale'],alpha=0.5)

plt.xlabel('NVSS factor ($\\alpha=0.7$)')
plt.ylabel('Number counts factor')
plt.plot([0,1.8],[0,1.8],color='orange')
print np.mean(t['nvss_scale']/4.91),np.mean(t['scale'])
'''
rms=t['rms']/(t['nvss_scale']/5.91)
# scale to 8h
rms*=np.sqrt(t['tot_integration']/8)
#for i in range(2):
#    plt.subplot(1,2,i+1)
#    if i==0:
val='elevation_mean'
label='Mean elevation'
#    else:
#        val='decl'
#        label='Declination'
label+=' (degrees)'
#filter=t['avg_integration']<5

plt.scatter(t[val],rms, alpha=0.5,c=t['avg_integration'])
plt.colorbar(label='Mean integration time (h)')
#    filter=t['avg_integration']<5
#    plt.scatter(t[val][~filter],rms[~filter], alpha=0.5,color='blue',label='8h')
#    if i==0:
angle=np.linspace(20,80,200)
plt.plot(angle,95.0/np.sin(angle*np.pi/180)**2.0,color='orange',label='$cosec$^2$(\\theta)$')
#    else:
#        angle=np.linspace(0,90,200)
#        plt.plot(angle,88.0/np.cos((angle-53)*np.pi/180)**2.0,color='orange')
#
angles=[]
means=[]
for i in range(17):
    angle=i*5+2.5
    angles.append(angle)
    select=((t[val]>=(angle-2.5)) & (t[val]<=(angle+2.5)))
    mean=np.nanmedian(rms[select])
    means.append(mean)
plt.scatter(angles,means,color='red',marker='s',label='Median noise values')
plt.legend()
plt.ylim(0,400)
plt.xlabel(label)
plt.ylabel('Equivalent 8-h flux-scaled rms noise ($\mu$Jy)')
plt.show()
'''
rms=t['rms']/(t['nvss_scale']/5.91)
plt.scatter(t['decl'],rms)
angle=np.linspace(0,90,200)
plt.plot(angle,100.0/np.cos((angle-53.0)*np.pi/180)**2.0,color='orange')
for i in range(17):
    angle=i*5+2.5
    select=((t['decl']>=(angle-2.5)) & (t['decl']<=(angle+2.5)))
    mean=np.median(rms[select])
    plt.scatter(angle,mean,color='red')
plt.xlabel('Dec (degrees)')
plt.ylabel('rms noise (microJy)')
plt.ylim(0,400)
'''
#plt.scatter(t['tgss_scale'],t['nvss_scale'])
#plt.scatter(t['rms'],t['dr'])
#plt.scatter(t['catsources'],np.sqrt(t['first_ra']**2.0+t['first_dec']**2.0))
#print np.median(t['nvss_scale']),np.median(t['tgss_scale'])
#print np.median(t['nvss_scale']/t['scale']),np.median(t['tgss_scale']/t['scale'])
##print np.array(t['nvss_scale']/t['scale'],dtype=np.float64)
#plt.hist(np.log10(np.array(t['nvss_scale']/t['scale'],dtype=np.float64)),bins=15,alpha=0.5,label='NVSS')
#plt.hist(np.log10(np.array(t['tgss_scale']/t['scale'],dtype=np.float64)),bins=15,alpha=0.5,label='TGSS')
#plt.legend(loc=0)
#plt.xlabel('log10(flux scale ratio)')
#plt.hist(t['catsources'],bins=20)
plt.show()

    
