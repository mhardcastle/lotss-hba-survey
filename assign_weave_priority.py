from surveys_db import SurveysDB

lines=open('/home/mjh/lgz_priority_regions.csv').readlines()
with SurveysDB() as sdb:
    for l in lines[1:]:
        bits=l.rstrip().split(',')
        print bits
        command='update fields set weave_priority=%s where ra>=%s and ra<=%s and decl>=%s and decl<=%s and abs(gal_b)>%s' % (bits[5],bits[0],bits[1],bits[2],bits[3],bits[4])
        print command
        sdb.cur.execute(command)
        
    
