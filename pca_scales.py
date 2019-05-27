from surveys_db import SurveysDB
from astropy.table import Table
import numpy as np
from sklearn.decomposition import PCA
import matplotlib.pyplot as plt

with SurveysDB() as sdb:
    sdb.cur.execute('select fields.id,fields.ra,fields.decl,quality.* from quality left join fields on fields.id=quality.id where fields.status!="Failed"')
    results=sdb.cur.fetchall()

td={}
for key in results[0]:
    if '.' in key:
        continue
    print key
    vl=[]
    for r in results:
        vl.append(r[key])
    td[key]=vl
t=Table(td)



pca = PCA(n_components=1)
X=np.array([t['tgss_scale'],t['nvss_scale']/4.91,t['scale']]).T
print X.shape
print pca.fit(X)
print pca.mean_
print pca.explained_variance_ratio_
print pca.components_

weighted=np.sum(X/pca.components_,axis=1)/np.sum(1/pca.components_)
print weighted
print np.std(weighted), np.std(X[:,0]), np.std(X[:,1]), np.std(X[:,2])

plt.scatter(weighted,X[:,0],alpha=0.5,label='TGSS')
plt.scatter(weighted,X[:,1],alpha=0.5,label='NVSS')
plt.scatter(weighted,X[:,2],alpha=0.5,label='Counts')
plt.plot([0,1],[0,1])
plt.legend(loc=0)
plt.show()

