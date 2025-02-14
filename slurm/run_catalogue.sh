#!/bin/bash 
#SBATCH -N 1                  # number of nodes
#SBATCH -c 16                 # number of cores
#SBATCH --ntasks=1            # number of tasks
#SBATCH -t 8:00:00           # maximum run time in [HH:MM:SS] or [MM:SS] or [minutes]

## submit the job with OBSID as an argument
OBSID=${1}

#################################################################################
## Cluster specific directories to change
## PLEASE SEE slurm/add_these_to_bashrc.txt

export SURVEYDIR=${SOFTWAREDIR}/lotss-hba-survey
export BINDPATHS=${SOFTWAREDIR},${DATA_DIR}

#################################################################################
## IN GENERAL DO NOT TOUCH ANYTHING BELOW HERE

## define the data directories
DATADIR=${DATA_DIR}/${OBSID}/selfcal

cd ${DATADIR}

apptainer exec -B ${PWD},${BINDPATHS} --no-home ${LOFAR_SINGULARITY} python3 ${SURVEYDIR}/lotsshr_catalogue.py ${OBSID} > catalogue.log 2>&1

