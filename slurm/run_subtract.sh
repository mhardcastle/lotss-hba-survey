#!/bin/bash 
#SBATCH -A do011
#SBATCH --output=/cosma8/data/do011/dc-mora2/logs/R-%x.%j.out  ### CLUSTER SPECIFIC

export TOIL_SLURM_ARGS="--export=ALL --job-name delaycal -p cosma8-dine2"

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
DATADIR=${DATA_DIR}/${OBSID}/concatenate-flag
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

## temporary, for lotss-subtract
export FLOCSDIR=/cosma8/data/do011/dc-mora2/processing/flocs

## go to working directory
cd ${OUTDIR}

## list of measurement sets - THIS WILL NEED TO BE CHECKED
singularity exec -B ${PWD},${BINDPATHS} --no-home ${LOFAR_SINGULARITY} python3 ${FLOCSDIR}/runners/create_ms_list.py VLBI lotss-subtract --ms_suffix .ms --solsdir ${DDFSOLSDIR}/SOLSDIR --ddf_rundir ${DATA_DIR}/${OBSID}/ddfpipeline ${DATADIR} >> create_ms_list.log 2>&1
## default options:
## --box_size 2.5 --freqavg 1 --timeavg 1 --ncpu 24 --chunkhours 0.5


########################

# MAKE TOIL STRUCTURE

# make folder for running toil
WORKDIR=${OUTDIR}/workdir
OUTPUT=${OUTDIR}
JOBSTORE=${OUTDIR}/jobstore
LOGDIR=${OUTDIR}/logs
TMPD=${OUTDIR}/tmpdir

mkdir -p ${TMPD}_interm
mkdir -p $WORKDIR
mkdir -p $OUTPUT
mkdir -p $LOGDIR


########################

# RUN TOIL

toil-cwl-runner \
--no-read-only \
--retryCount 0 \
--singularity \
--disableCaching \
--writeLogsFromAllJobs True \
--logFile ${OUTDIR}/job_output.txt \
--writeLogs ${LOGDIR} \
--outdir ${OUTPUT} \
--tmp-outdir-prefix ${TMPD}/ \
--jobStore ${JOBSTORE} \
--workDir ${WORKDIR} \
--coordinationDir ${OUTPUT} \
--tmpdir-prefix ${TMPD}_interm/ \
--disableAutoDeployment True \
--bypass-file-store \
--preserve-entire-environment \
--batchSystem slurm \
${VLBIDIR}/workflows/lotss-subtract.cwl mslist_VLBI_delay_calibration.json
#--cleanWorkDir never \ --> for testing




echo LINC starting
TMPID=`echo ${OBSID} | cut -d'/' -f 1`
echo export PYTHONPATH=\$LINC_DATA_ROOT/scripts:\$PYTHONPATH > tmprunner_${TMPID}.sh
echo 'cwltool --parallel --preserve-entire-environment --no-container --tmpdir-prefix=${TMPDIR} --outdir=${OUTDIR} --log-dir=${LOGSDIR} ${VLBIDIR}/workflows/concatenate-flag.cwl mslist_VLBI_concatenate-flag.json' >> tmprunner_${TMPID}.sh
(time singularity exec -B ${PWD},${BINDPATHS} --no-home ${LOFAR_SINGULARITY} bash tmprunner_${TMPID}.sh 2>&1) | tee ${OUTDIR}/job_output.txt
echo LINC ended
if grep 'Final process status is success' ${OUTDIR}/job_output.txt
then 
	echo 'SUCCESS: Pipeline finished successfully' > ${OUTDIR}/finished.txt
else
	echo "**FAILURE**: Pipeline failed with exit status: ${?}" > ${OUTDIR}/finished.txt
fi

