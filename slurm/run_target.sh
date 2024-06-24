#!/bin/bash 
#SBATCH -N 1                  # number of nodes
#SBATCH -c 32                 # number of cores
#SBATCH --ntasks=1            # number of tasks
#SBATCH -t 12:00:00           # maximum run time in [HH:MM:SS] or [MM:SS] or [minutes]
#SBATCH -p normal             # partition (queue); job can run up to 3 days
#SBATCH --output=/project/lofarvlbi/Share/surveys/logs/R-%x.%j.out

## submit the job with OBSID as an argument
OBSID=${1}

#################################################################################
## Cluster specific directories to change
## PLEASE SEE slurm/add_these_to_bashrc.txt 

export LINCDIR=${SOFTWAREDIR}/LINC
export FLOCSDIR=${SOFTWAREDIR}/flocs
BINDPATHS=${SOFTWAREDIR},${DATA_DIR}

#################################################################################
## IN GENERAL DO NOT TOUCH ANYTHING BELOW HERE

## define the data directories
DATADIR=${DATA_DIR}/${OBSID}
PROCDIR=${DATA_DIR}/processing
OUTDIR=${PROCDIR}/${OBSID}
TMPDIR=${PROCDIR}/${OBSID}/tmp/
LOGSDIR=${OUTDIR}/logs
mkdir -p ${TMPDIR}
mkdir -p ${LOGSDIR}

## location of LINC
LINC_DATA_ROOT=${LINCDIR}

# Pass along necessary variables to the container.
CONTAINERSTR=$(singularity --version)
if [[ "$CONTAINERSTR" == *"apptainer"* ]]; then
    export APPTAINERENV_LINC_DATA_ROOT=${LINC_DATA_ROOT}
    export APPTAINERENV_LOGSDIR=${LOGSDIR}
    export APPTAINERENV_TMPDIR=${TMPDIR}
    export APPTAINERENV_PREPEND_PATH=${LINC_DATA_ROOT}/scripts
else
    export SINGULARITYENV_LINC_DATA_ROOT=${LINC_DATA_ROOT}
    export SINGULARITYENV_LOGSDIR=${LOGSDIR}
    export SINGULARITYENV_TMPDIR=${TMPDIR}
    export SINGULARITYENV_PREPEND_PATH=${LINC_DATA_ROOT}/scripts
fi

## go to working directory
cd ${OUTDIR}

## pipeline input
singularity exec -B ${PWD},${BINDPATHS} --no-home ${LOFAR_SINGULARITY} python ${FLOCSDIR}/runners/create_ms_list.py LINC target --cal_solutions ${DATADIR}/LINC-cal_solutions.h5 ${DATADIR} > create_mslist.log

echo LINC starting
TMPID=`echo ${OBSID} | cut -d'/' -f 1`
echo export PYTHONPATH=\$LINC_DATA_ROOT/scripts:\$PYTHONPATH > tmprunner_${TMPID}.sh
echo 'cwltool --parallel --preserve-entire-environment --no-container --tmpdir-prefix=${TMPDIR} --outdir=${OUTDIR} --leave-tmpdir --log-dir=${LOGSDIR} ${LINC_DATA_ROOT}/workflows/HBA_target.cwl mslist_LINC_target.json' >> tmprunner_${TMPID}.sh
(time singularity exec -B ${PWD},${BINDPATHS} --no-home ${LOFAR_SINGULARITY} bash tmprunner_${TMPID}.sh 2>&1) | tee ${OUTDIR}/job_output.txt
echo LINC ended
if grep 'Final process status is success' ${OUTDIR}/job_output.txt
then 
	echo 'SUCCESS: Pipeline finished successfully' > ${OUTDIR}/finished.txt
else
	echo "**FAILURE**: Pipeline failed with exit status: ${?}" > ${OUTDIR}/finished.txt
fi

