#!/usr/bin/python

from __future__ import print_function
from time import sleep
import datetime
from surveys_db import SurveysDB, tag_field, get_cluster
import os
import argparse

def main(name,status,stage_id=None,time=None,workdir=None,av=None,survey=None):
    # adapted from surveys_db
    # utility function to just update the status of a field
    # name can be None (work it out from cwd), or string (field name)
    id=name 
    with SurveysDB(survey=survey) as sdb:
        idd=sdb.db_get('lb_calibrators',id)
        if idd is None:
          raise RuntimeError('Unable to find database entry for field "%s".' % id)
        idd['status']=status
        tag_field(sdb,idd,workdir=workdir)
        if time is not None and idd[time] is None:
            idd[time]=datetime.datetime.now()
        if stage_id is not None:
            idd['staging_id']=stage_id
        sdb.db_set('lb_calibrators',idd)
    sdb.close()        

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('field_id')
    parser.add_argument('status')
    parser.add_argument('--stage_id',type=int, default=None)
    parser.add_argument('--time', type=str, default=None)
    parser.add_argument('--workdir', type=str, default=None)
    parser.add_argument('--av', type=str, default=None)
    parser.add_argument('--survey', type=str, default=None)

    args = parser.parse_args()

    main(args.field_id, args.status, stage_id=args.stage_id, time=args.time, workdir=args.workdir, av=args.av, survey=args.survey )
