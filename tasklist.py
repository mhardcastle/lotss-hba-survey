# Code to maintain a list of tasks per obsid

from surveys_db import SurveysDB

def set_task_list(obsid,tlist):
    with SurveysDB() as sdb:
        sdb.execute('delete from lb_operations where id=%s',(obsid,))
        for i,v in enumerate(tlist):
            sdb.execute('insert into lb_operations values (%s,%s,%s,0)',(obsid,v,i))

def get_task_list(obsid):
    with SurveysDB(readonly=True) as sdb:
        sdb.execute('select * from lb_operations where id=%s and done=0 order by rank',(obsid,))
        results=sdb.cur.fetchall()
    return [r['operation'] for r in results]

def mark_done(obsid,operation,done=1):
    with SurveysDB() as sdb:
        sdb.execute('update lb_operations set done=%s where id=%s and operation=%s',(done,obsid,operation))


if __name__=='__main__':
    print('Testing operations!')
    set_task_list(1,['something','something else','another thing'])
    print(get_task_list(1))
    mark_done(1,'something')
    print('next task is',get_task_list(1)[0])
    
