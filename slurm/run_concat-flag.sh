#!/bin/bash 
#SBATCH -N 1                  # number of nodes
#SBATCH -c 32                 # number of cores  ### CLUSTER SPECIFIC
#SBATCH --ntasks=1            # number of tasks
#SBATCH -t 128:00:00           # maximum run time in [HH:MM:SS] or [MM:SS] or [minutes]
#SBATCH -p normal             # partition (queue); job can run up to 3 days  ### CLUSTER SPECIFIC
#SBATCH --output=/project/lofarvlbi/Share/surveys/logs/R-%x.%j.out  ### CLUSTER SPECIFIC

## submit the job with OBSID as an argument
OBSID=${1}

#################################################################################
## Cluster specific directories to change
## PLEASE SEE slurm/add_these_to_bashrc.txt 

export VLBIDIR=${SOFTWAREDIR}/VLBI-cwl
export LINCDIR=${SOFTWAREDIR}/LINC
export FLOCSDIR=${SOFTWAREDIR}/flocs
export LOFARHELPERS=${SOFTWAREDIR}/lofar_helpers
export FACETSELFCAL=${SOFTWAREDIR}/lofar_facet_selfcal
BINDPATHS=${SOFTWAREDIR},${DATA_DIR}

#################################################################################
## IN GENERAL DO NOT TOUCH ANYTHING BELOW HERE

## define the data directories
DATADIR=${DATA_DIR}/${OBSID}/setup
DDFSOLSDIR=${DATA_DIR}/${OBSID}/ddfsolutions
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
    export APPTAINERENV_PREPEND_PATH=${VLBIDIR}/scripts
    export APPTAINERENV_PYTHONPATH="$VLBIDIR/scripts:$LINCDIR/scripts:\$PYTHONPATH"
else
    export SINGULARITYENV_LINC_DATA_ROOT=${LINC_DATA_ROOT}
    export SINGULARITYENV_LOGSDIR=${LOGSDIR}
    export SINGULARITYENV_TMPDIR=${TMPDIR}
    export SINGULARITYENV_PREPEND_PATH=${LINC_DATA_ROOT}/scripts
    export SINGULARITYENV_PREPEND_PATH=${VLBIDIR}/scripts
    export SINGULARITYENV_PYTHONPATH="$VLBIDIR/scripts:$LINCDIR/scripts:\$PYTHONPATH"
fi



## go to working directory
cd ${OUTDIR}

## list of measurement sets - THIS WILL NEED TO BE CHECKED
singularity exec -B ${PWD},${BINDPATHS} ${LOFAR_SINGULARITY} python3 ${FLOCSDIR}/runners/create_ms_list.py VLBI concatenate-flag --ddf_solset ${DDFSOLSDIR}/SOLSDIR --linc ${LINCDIR} ${DATADIR}/ >> create_ms_list.log 2>&1


echo LINC starting
echo export PYTHONPATH=\$LINC_DATA_ROOT/scripts:\$PYTHONPATH > tmprunner_${OBSID}.sh
echo 'cwltool --parallel --preserve-entire-environment --no-container --tmpdir-prefix=${TMPDIR} --outdir=${OUTDIR} --log-dir=${LOGSDIR} ${VLBIDIR}/workflows/concatenate-flag.cwl mslist_VLBI_concatenate-flag.json' >> tmprunner_${OBSID}.sh
(time singularity exec -B ${PWD},${BINDPATHS} ${LOFAR_SINGULARITY} bash tmprunner_${OBSID}.sh 2>&1) | tee ${OUTDIR}/job_output.txt
echo LINC ended
if grep 'Final process status is success' ${OUTDIR}/job_output.txt
then 
	echo 'SUCCESS: Pipeline finished successfully' > ${OUTDIR}/finished.txt
else
	echo "**FAILURE**: Pipeline failed with exit status: ${?}" > ${OUTDIR}/finished.txt
fi

