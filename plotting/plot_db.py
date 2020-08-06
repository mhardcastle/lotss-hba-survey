import numpy as np
import pylab as plt
import pyfits
from matplotlib import rc
from surveys_db import SurveysDB
import requests
from lxml import html
import shutil
import os.path

def sepn(r1,d1,r2,d2):

    """
    Calculate the separation between 2 sources, RA and Dec must be
    given in radians. Returns the separation in radians [TWS]
    """

    # NB slalib sla_dsep does this
    # www.starlink.rl.ac.uk/star/docs/sun67.htx/node72.html
    
    cos_sepn=np.sin(d1)*np.sin(d2) + np.cos(d1)*np.cos(d2)*np.cos(r1-r2)
    sepn = np.arccos(cos_sepn)
    
    return sepn    

#Define various angle conversion factors (multiply to undertake operation)
arcsec2deg=1.0/3600
arcmin2deg=1.0/60
deg2rad=np.pi/180
deg2arcsec = 1.0/arcsec2deg
rad2deg=180.0/np.pi
arcmin2rad=arcmin2deg*deg2rad
arcsec2rad=arcsec2deg*deg2rad
rad2arcmin=1.0/arcmin2rad
rad2arcsec=1.0/arcsec2rad
steradians2degsquared = (180.0/np.pi)**2.0
degsquared2steradians = 1.0/steradians2degsquared


fontsize=16 # adjust to taste
rc('font',**{'family':'serif','serif':['Times'],'size':fontsize})
rc('text', usetex=True)


ravals = []
decvals = []
pointings = []
griddict = {}
infile = open('LoTSS_grid.hba.txt','r')
for line in infile:
    line = line[:-1]
    line = line.split(' ')
    while '' in line:
        line.remove('')
    pointings.append(line[0])
    ravals.append(float(line[1]))
    decvals.append(float(line[2]))
    griddict[line[0]] = [line[1],line[2]]
infile.close()

ravals = np.array(ravals)
decvals = np.array(decvals)
projection='aitoff'
RA = ravals
Dec = decvals
org = 180
colour = 'k'
x = np.remainder(RA+360-org,360) # shift RA values
ind = x>180
x[ind] -=360    # scale conversion to [-180, 180]
x=-x    # reverse the scale: East to the left
tick_labels = np.array([150, 120, 90, 60, 30, 0, 330, 300, 270, 240, 210])
tick_labels = np.remainder(tick_labels+360+org,360)
#tick_labels = ['2h','4h','6h','8h','10h','12h','14h','16h','18h','20h','22h','24h']
#tick_labels = ['10h','8h','6h','4h','2h','0h','22h','20h','16h','14h','12h']
tick_labels = list(tick_labels)
for i in range(0,len(tick_labels)):
    #tick_labels[i] = str(tick_labels[i])+'        '
    tick_labels[i] = ''+str(tick_labels[i])+'\ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \,'
    print tick_labels[i]
    #.replace('h','h        ')
fig = plt.figure(figsize=(10, 5))
ax = fig.add_subplot(111, projection=projection)
    

ravals = np.array(ravals)
decvals = np.array(decvals)

RA = ravals
Dec = decvals
org = 180
x = np.remainder(RA+360-org,360) # shift RA values
ind = x>180
x[ind] -=360    # scale conversion to [-180, 180]
x=-x    # reverse the scale: East to the left
ax.scatter(np.radians(x),np.radians(Dec),s=1,color='k',zorder=0,alpha=0.5)  # convert degrees to radians

ravals = []
decvals = []
infile = open('DR2-pointings.txt','r')
for line in infile:
    line = line[:-1]
    line = line.split(' ')
    while '' in line:
        line.remove('')
    pointing = line[0]
    ravals.append(float(line[1]))
    decvals.append(float(line[2]))
    if griddict[pointing][0] != line[1] and griddict[pointing][1] != line[2]:
	print pointing, griddict[pointing], line[1],line[2], '***in the wrong place and DR2',sepn(float(griddict[pointing][0])*deg2rad,float(griddict[pointing][1])*deg2rad,float(line[1])*deg2rad,float(line[2])*deg2rad)*rad2deg
infile.close()


ravals = np.array(ravals)
decvals = np.array(decvals)

RA = ravals
Dec = decvals
org = 180
x = np.remainder(RA+360-org,360) # shift RA values
ind = x>180
x[ind] -=360    # scale conversion to [-180, 180]
x=-x    # reverse the scale: East to the left
ax.scatter(np.radians(x),np.radians(Dec),s=5,marker='+',color='k',zorder=5,alpha=1)  # convert degrees to radians


ravals = []
decvals = []
identities = []
identities_fields = []
progressdict = {}
infile = open('findcal.log','r')
for line in infile:
    line = line[:-1]
    line = line.split(',')
    while '' in line:
        line.remove('')
    if 'PNK3-17' in line[0]:
        continue
    if 'P' in line[0][0]:
        identities_fields.append(line[0])
        identities.append(line[1])
        ravals.append(float(line[2]))
        decvals.append(float(line[3]))
        progressdict[line[0]] = [line[2],line[3],'observed',line[4]]
	pointing = line[0]
        sep = sepn(float(griddict[pointing][0])*deg2rad,float(griddict[pointing][1])*deg2rad,float(line[2])*deg2rad,float(line[3])*deg2rad)*rad2deg
        if sep > 0.05:
            print pointing, griddict[pointing], line[2],line[3], '***in the wrong place',sep
infile.close()

ravals = np.array(ravals)
decvals = np.array(decvals)

RA = ravals
Dec = decvals
org = 180
x = np.remainder(RA+360-org,360) # shift RA values
ind = x>180
x[ind] -=360    # scale conversion to [-180, 180]
x=-x    # reverse the scale: East to the left
ax.scatter(np.radians(x),np.radians(Dec),s=10,color='r',zorder=1,alpha=1.0)  # convert degrees to radians


unspecifieddata = {}
#LC2_038
unspecifieddata['P8Hetdex'] = [233998,None,8,2.01326592,244,8.0,'3C196',233994,None,8,2.01326592,244,'CS013HBA*'] # Should have been P8Hetdex21
unspecifieddata['P5Hetdex'] = [233996,None,8,2.01326592,244,8.0,'3C196',233994,None,8,2.01326592,244,'CS013HBA*'] # Should have been P5Hetdex21

#LT5_007
unspecifieddata['P236+53'] = [470106,None,16,1.00139008,243,8.0,'3C295',470520,None,16,1.00139008,243,''] 

#LC6_015
unspecifieddata['P138+52'] = [589743,None,16,1.00139008,243,8.0,'UNKNOWN',589747,None,16,1.00139008,243,'']
unspecifieddata['P007+33'] = [529865,None,16,1.00139008,243,8.0,'UNKNOWN',529869,None,16,1.00139008,243,'']
unspecifieddata['P121+32'] = [523776,None,16,1.00139008,243,8.0,'UNKNOWN',523770,None,16,1.00139008,243,'']

#LC7_024
unspecifieddata['P212+60'] = [612934,None,16,1.00139008,243,8.0,'UNKNOWN',612940,None,16,1.00139008,243,'']

#LC7_025
unspecifieddata['P072+04'] = [584443,None,16,1.00139008,243,4.0,'UNKNOWN',584437,None,16,1.00139008,243,'']

#LC8_004
unspecifieddata['P091+22'] = [625186,None,16,1.00139008,243,8.0,'UNKNOWN',625190,None,16,1.00139008,243,'']

#LC8_022
unspecifieddata['P199+02'] = [626180,None,16,1.00139008,243,4.0,'UNKNOWN',626174,None,16,1.00139008,243,'']
unspecifieddata['P032+41'] = [619658,None,16,1.00139008,243,8.0,'UNKNOWN',619662,None,16,1.00139008,243,'']

#LC8_025
unspecifieddata['P121+22'] = [619778,None,16,1.00139008,243,8.0,'UNKNOWN',619782,None,16,1.00139008,243,'']

#LC8_030
unspecifieddata['P147+29'] = [610392,None,16,1.00139008,243,8.0,'UNKNOWN',610386,None,16,1.00139008,243,'']

#LC9_014
unspecifieddata['P336+01'] = [626972,None,16,1.00139008,243,4.0,'UNKNOWN',626976,None,16,1.00139008,243,'']
ravals = []
decvals = []
for pointing in unspecifieddata:
    ravals.append(float(griddict[pointing][0]))
    decvals.append(float(griddict[pointing][1]))
    identities.append(unspecifieddata[pointing][0])
    identities_fields.append(pointing)
    progressdict[pointing] = [float(griddict[pointing][0]),float(griddict[pointing][0]),'observed','UNKNOWN']
ravals = np.array(ravals)
decvals = np.array(decvals)

RA = ravals
Dec = decvals
org = 180
x = np.remainder(RA+360-org,360) # shift RA values
ind = x>180
x[ind] -=360    # scale conversion to [-180, 180]
x=-x    # reverse the scale: East to the left
ax.scatter(np.radians(x),np.radians(Dec),s=10,color='r',zorder=1,alpha=1.0) # convert degrees to radians





sdb = SurveysDB()
ravals = []
decvals = []
for i in range(0,len(identities)):
    obsdict = sdb.get_observation(identities[i])

    status = obsdict['status']
    if status == 'DI_processed' or status == 'DI_Processed':
        fielddict = sdb.get_field(identities_fields[i])
        ravals.append(fielddict['ra'])
        decvals.append(fielddict['decl'])
        progressdict[identities_fields[i]][2] = 'partly processed'
sdb.close()

ravals = np.array(ravals)
decvals = np.array(decvals)

RA = ravals
Dec = decvals
org = 180
x = np.remainder(RA+360-org,360) # shift RA values
ind = x>180
x[ind] -=360    # scale conversion to [-180, 180]
x=-x    # reverse the scale: East to the left
ax.scatter(np.radians(x),np.radians(Dec),s=10,color='b',zorder=2,alpha=1.0)  # convert degrees to radians


sdb = SurveysDB()
ravals = []
decvals = []
for i in range(0,len(identities)):
    fielddict = sdb.get_field(identities_fields[i])
    status = fielddict['status']
    if status == 'Complete' or status == 'Archived':
        ravals.append(fielddict['ra'])
        decvals.append(fielddict['decl'])
        progressdict[identities_fields[i]][2] = 'fully processed'
sdb.close()

ravals = np.array(ravals)
decvals = np.array(decvals)

RA = ravals
Dec = decvals
org = 180
x = np.remainder(RA+360-org,360) # shift RA values
ind = x>180
x[ind] -=360    # scale conversion to [-180, 180]
x=-x    # reverse the scale: East to the left
ax.scatter(np.radians(x),np.radians(Dec),s=10,color='g',zorder=3,alpha=1.0)  # convert degrees to radians


print tick_labels
ax.set_xticklabels(tick_labels,verticalalignment='top',rotation='vertical')     # we add the scale on the x axis

    
#ax.set_title(title)
#ax.title.set_fontsize(15)
ax.set_xlabel("RA")
ax.xaxis.label.set_fontsize(fontsize)
ax.set_ylabel("Dec")
ax.yaxis.label.set_fontsize(fontsize)
ax.xaxis.set_tick_params(pad=18.)
ax.xaxis.labelpad = -110
ax.yaxis.labelpad = -10
ax.grid(True)



plt.savefig('Tier1-HBA-surveypointings-progress.png',dpi=250)


for element in progressdict:
    print element,progressdict[element][0],progressdict[element][1],progressdict[element][2],progressdict[element][3]
