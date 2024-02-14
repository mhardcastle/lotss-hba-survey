# Code to maintain a list of tasks per field

from surveys_db import SurveysDB

def set_task_list(field,tlist):
    with SurveysDB() as sdb:
        sdb.execute('delete from lb_operations where id=%s',(field,))
        for i,v in enumerate(tlist):
            sdb.execute('insert into lb_operations values (%s,%s,%s,0)',(field,v,i))

def get_task_list(field):
    with SurveysDB(readonly=True) as sdb:
        sdb.execute('select * from lb_operations where id=%s and done=0 order by rank',(field,))
        results=sdb.cur.fetchall()
    return [r['operation'] for r in results]

def mark_done(field,operation,done=1):
    with SurveysDB() as sdb:
        sdb.execute('update lb_operations set done=%s where id=%s and operation=%s',(done,field,operation))


if __name__=='__main__':
    print('Testing operations!')
    set_task_list('testfield',['something','something else','another thing'])
    print(get_task_list('testfield'))
    mark_done('testfield','something')
    print('next task is',get_task_list('testfield')[0])
    
