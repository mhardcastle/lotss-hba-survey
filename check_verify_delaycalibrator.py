#!/usr/bin/python

import argparse
from surveys_db import SurveysDB, tag_field, get_cluster
from lbfields_utils import update_status, get_local_obsid
import os

def main( obsid='', solutions='' ):
    user = os.getenv('USER')
    if len(user) > 20:
        user = user[0:20]
    cluster = os.getenv('DDF_PIPELINE_CLUSTER')
    basedir = os.getenv('DATA_DIR')
    procdir = os.path.join(basedir,'processing')

    ## check the databse for things that have DelayCheck status
    with SurveysDB(readonly=True) as sdb:
        sdb.cur.execute('select * from lb_fields where clustername="'+cluster+'" and username="'+user+'" and status="DelayCheck"')
        result=sdb.cur.fetchall()

    ## if run without arguments, just checking if there are solutions to look at
    if obsid == '' and solutions == '':
        if len(result) == 0:
            print('There are currently no delay calibrator solutions to check.')
        else:
            print('Please check the following delay calibrator solutions and then run this script again when you are ready to verify the solutions:')
            for r in result:
                loc = r['location']
                print('Field: {:s} .... with solutions at {:s}/{:s}/phaseup-concat/'.format(r['id'],os.path.join( basedir,r['id'] ),os.path.basename(loc)))
    else:
        if obsid == '' or solutions == '':
            print('You have provided either the observation id OR the solutions file, please provide both!')
        else:
            print('You are trying to validate {:s} with the following solutions:'.format(obsid))
            print('  {:s}'.format(solutions))
            response = input("Are you sure?: (yes/no)")
            if response.capitalize() == 'Yes':
                for r in result:
                    loc = r['location']
                    if obsid == os.path.basename(loc):
                        field = r['id']
                        phaseupconcatpath = '{:s}/{:s}/phaseup-concat'.format(os.path.join(basedir,r['id']),os.path.basename(loc))
                        solutionspath = os.path.dirname(solutions)
                        if phaseupconcatpath == solutionspath:
                            ## the first solutions are suitable! nothing to be done
                            print('Accepting the pipeline version of the solutions.')
                        else:
                            oldfiles = glob.glob(os.path.join(phaseupconcatpath,'*'))
                            defaultdir = os.path.join(phaseupconcatpath,'pipelinesols') 
                            os.makedirs()
                            for oldf in oldfiles:
                                os.system('mv {:s} {:s}'.format(oldf, os.path.join(defaultdir,os.path.basename(oldf)) )
                            ## also want to move plots etc, assume in os.path.dirname(solutions)
                            
                            os.system('mv {:s} {:s}'.format(solutions, os.path.join(phaseupconcatpath,os.path.basename(solutions))

                        field_procdir = os.path.join( procdir, '{:s}/{:s}'.format(field,obsid) )
                        os.makedirs(field_procdir,exist_ok=True)
                        with open( os.path.join(field_procdir,'finished.txt') ) as f:
                            f.write("SUCCESS: Pipeline finished successfully")
                        with open( os.path.join(procdir,'job_output.txt') ) as f:
                            f.write("delay.cwl Resolved \n\n\n\n\n\n\n\n\n\n")    
                        ## move solutions to the procdir

                        ## update statuses
                        mark_done(obsid,'delay')
                        update_status(field,'Queued')

            else:
                print('Solutions not validated.')

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('--obsid', type=str, help='observation ID (without the L)', default='')
    parser.add_argument('--solutions', type=str, help='absolute path of h5parm to use', default='')

    args = parser.parse_args()
    main( args.obsid, args.solutions )
