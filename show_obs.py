from surveys_db import SurveysDB

with SurveysDB(readonly=True) as sdb:
    sdb.cur.execute('select * from observations order by field')
    result=sdb.cur.fetchall()

for r in result:
    print "%8s %-20s %-20s %-12s %s" % ( r['id'],r['field'],r['status'],r['project_code'],r['date'] )
    
