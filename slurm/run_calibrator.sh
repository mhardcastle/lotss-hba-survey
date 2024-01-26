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
MYSINGULARITYDIR=/project/lofarvlbi/Software/singularity
FLOCSDIR=/project/lofarvlbi/Software/flocs
BINDPATHS=/project/lofarvlbi/Software,/project/lofarvlbi/Share/surveys

## Singularity version
SIMG=${MYSINGULARITYDIR}/lofar_sksp_v4.2.3_znver2_znver2_aocl4_debug.sif
#################################################################################
## IN GENERAL DO NOT TOUCH ANYTHING BELOW HERE

## define the data directories
DATADIR=${LINC_DATA_DIR}/${OBSID}
PROCDIR=${LINC_DATA_DIR}/processing
OUTDIR=${PROCDIR}/${OBSID}
TMPDIR=${PROCDIR}/${OBSID}/tmp/
LOGSDIR=${OUTDIR}/logs
mkdir -p ${TMPDIR}
mkdir -p ${LOGSDIR}

## location of LINC
LINC_DATA_ROOT=${LINC_INSTALL_DIR}

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
singularity exec -B ${PWD},${BINDPATHS} ${SIMG} python ${FLOCSDIR}/runners/create_ms_list.py ${DATADIR}

echo LINC starting
echo export PYTHONPATH=\$LINC_DATA_ROOT/scripts:\$PYTHONPATH > tmprunner_${OBSID}.sh
echo 'cwltool --parallel --preserve-entire-environment --no-container --tmpdir-prefix=${TMPDIR} --outdir=${OUTDIR} --leave-tmpdir --log-dir=${LOGSDIR} ${LINC_DATA_ROOT}/workflows/HBA_calibrator.cwl mslist.json' >> tmprunner_${OBSID}.sh
(time singularity exec -B ${PWD},${BINDPATHS} ${SIMG} bash tmprunner_${OBSID}.sh 2>&1) | tee ${OUTDIR}/job_output.txt
echo LINC ended
if grep 'Final process status is success' ${OUTDIR}/job_output.txt
then 
	echo 'SUCCESS: Pipeline finished successfully' > ${OUTDIR}/finished.txt
else
	echo "**FAILURE**: Pipeline failed with exit status: ${?}" > ${OUTDIR}/finished.txt
fi

