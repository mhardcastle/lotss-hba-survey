from surveys_db import SurveysDB

lines=open('/home/mjh/Transient_LOTTS.csv').readlines()

with SurveysDB() as sdb:
    sdb.cur.execute('delete from transients')
    for l in lines[1:]:
        bits=l.rstrip().split(',')
        name=bits[0]
        count=0
        while True:
            t=sdb.get_transient(name)
            if t is None:
                break
            name=bits[0]+'_%i' % (count+2)
            count=count+1
        sdb.cur.execute('insert into transients values ("%s",%s,%s,"%s")' % (name,bits[1],bits[2],bits[3]))
