from __future__ import print_function
from surveys_db import SurveysDB

# Utility functions for the LoTSS HBA survey, formerly part of surveys_db.py.

def get_next():
    # return the name of the top-priority field with appropriate status
    sdb=SurveysDB(readonly=True)
    sdb.cur.execute('select fields.id as id,sum(nsb*integration/232) as s,count(observations.id) as c,fields.priority,fields.lotss_field,fields.required_integration from fields left join observations on (observations.field=fields.id) where fields.status="Not started" and (observations.status="Archived" or observations.status="DI_processed") and (gal_b>10 or gal_b<-10 or gal_b is NULL or fields.priority>9) group by fields.id having (s>(0.95*fields.required_integration) or lotss_field=0) order by fields.priority desc,ra desc')
    results=sdb.cur.fetchall()
    sdb.close()
    if len(results)>0:
        return results[0]['id']
    else:
        return None

def get_next_selfcalibration():
    sdb=SurveysDB(readonly=True)
    sdb.cur.execute('select reprocessing.id,reprocessing.priority,reprocessing.fields,reprocessing.extract_status from reprocessing where reprocessing.selfcal_status like "%SREADY%" group by reprocessing.priority desc')
    results=sdb.cur.fetchall()
    sdb.close()
    if len(results)>0:
        return results[0]
    else:
        return None

def get_next_extraction():
    # return the name of the top-priority field with appropriate status
    sdb=SurveysDB(readonly=True)
    sdb.cur.execute('select * from reprocessing where reprocessing.extract_status like "%EREADY%" group by reprocessing.priority desc')
    results=sdb.cur.fetchall()
    #print(results[0])
    sdb.close()

    if len(results)==0:
        return None
    
    # Find next field for target
    fields = results[0]['fields'].split(',')
    extract_status = results[0]['extract_status'].split(',')
    try:
        bad_pointings = results[0]['bad_pointings'].split(',')
    except (AttributeError,KeyError):
        bad_pointings = ['']

    for i in range(0,len(fields)):
        if extract_status[i] != 'EREADY':
            continue
        field = fields[i]
        if field in bad_pointings:
            print('Field',field,'in bad pointings -- skipping and setting to BADP')
            sdb=SurveysDB()
            extractdict = sdb.get_reprocessing(results[0]['id'])
            extract_status[i] = 'BADP'
            extractdict['extract_status'] = ','.join(extract_status)
            sdb.db_set('reprocessing',extractdict)
            sdb.close()
            continue
        seli = i
    print('Next extraction:',results[0]['id'],fields[seli])

    return  results[0]['id'],fields[seli],results[0]['ra'],results[0]['decl'],results[0]['size']    
    
def update_reprocessing_extract(name,field,status):
    with SurveysDB() as sdb:
        extractdict = sdb.get_reprocessing(name)
        desindex = extractdict['fields'].split(',').index(field)
        splitstatus = extractdict['extract_status'].split(',')
        splitstatus[desindex] = status
        extractdict['extract_status'] = ','.join(splitstatus)
        sdb.db_set('reprocessing',extractdict)

