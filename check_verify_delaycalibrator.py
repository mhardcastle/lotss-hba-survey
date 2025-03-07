#!/usr/bin/python

import argparse
from surveys_db import SurveysDB, tag_field, get_cluster
from lbfields_utils import update_status, get_local_obsid
from tasklist import mark_done
import os
import glob

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
                obsid = get_local_obsid( r['id'] )[0]
                print('Field: {:s} .... with solutions at {:s}/{:s}/phaseup-concat/'.format(r['id'],os.path.join( basedir,r['id'] ),obsid))
    else:
        if obsid == '' or solutions == '':
            print('You have provided either the observation id OR the solutions file, please provide both!')
        else:
            if solutions[0] != '/':
                solutions = f"{os.getcwd()}/{solutions}"
            print('You are trying to validate {:s} with the following solutions:'.format(obsid))
            print('  {:s}'.format(solutions))
            response = input("Are you sure?: (yes/no)")
            if response.capitalize() == 'Yes':
                #Assume there may be multiple fields ready for DelayCheck
                for r in result:
                    field = r['id']
                    local_obsids = get_local_obsid(field)
                    for local_obsid in local_obsids:
                        if obsid == local_obsid:
                            #Found the field matching obsid
                            phaseupconcatpath = os.path.join( basedir, field, obsid, 'phaseup-concat' )
                            solutionspath = os.path.dirname(solutions)
                            if phaseupconcatpath == solutionspath:
                                ## the first solutions are suitable! nothing to be done
                                print('Accepting the pipeline version of the solutions, cleaning up.')
                                os.system('cp {:s} {:s}'.format(solutions, os.path.join(phaseupconcatpath,os.path.basename(solutions).replace('.h5','_verified.h5') ) ) )
                            else:
                                oldfiles = glob.glob(os.path.join(phaseupconcatpath,'*'))
                                defaultdir = os.path.join(phaseupconcatpath,'pipelinesols') 
                                os.makedirs(defaultdir, exist_ok=True)
                                for oldf in oldfiles:
                                    os.system('mv {:s} {:s}'.format(oldf, os.path.join(defaultdir,os.path.basename(oldf)) ) )
                                ## copy solutions
                                os.system('cp {:s} {:s}'.format(solutions, os.path.join(phaseupconcatpath, os.path.basename(solutions).replace('.h5','_verified.h5') ) ) )

                            ## also want to move plots etc, assume in os.path.dirname(solutions)
                            newfiles = glob.glob(os.path.join(solutionspath,'plotlosoto*/*'))
                            if len(newfiles) == 0:
                                newfiles = glob.glob(os.path.join(solutionspath,'*009*png'))
                            verifieddir = os.path.join(phaseupconcatpath,'verified_solution_plots')
                            os.makedirs(verifieddir, exist_ok=True)
                            for newf in newfiles:
                                 os.system('mv {:s} {:s}'.format(newf, os.path.join(verifieddir, os.path.basename(newf)) ) )
                            # update statuses
                            print('Delay solutions verified and have been copied to: {:s}'.format( os.path.join(phaseupconcatpath, os.path.basename(solutions).replace('.h5','_verified.h5') ) ) )
                            print(f'Updating status for {field}')
                            mark_done(obsid,'delay')
                            update_status(field,'Unpacked')  ## so it goes back to getting checked for next task

            else:
                print('Solutions not validated.')

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('--obsid', type=str, help='observation ID (without the L)', default='')
    parser.add_argument('--solutions', type=str, help='absolute path of h5parm to use', default='')

    args = parser.parse_args()
    main( args.obsid, args.solutions )
