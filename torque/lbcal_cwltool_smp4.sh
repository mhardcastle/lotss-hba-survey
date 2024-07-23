#!/bin/bash

# To be run inside the singularity!

export LBCAL_DIR=/home/mjh/pipeline-lbcal
export LINC_INSTALL_DIR=/home/mjh/pipeline-lbcal/LINC
export LINC_DATA_ROOT=$LINC_INSTALL_DIR
export FLOCS_DIR=/home/mjh/git/flocs

export PYTHONPATH=${LINC_INSTALL_DIR}/scripts:$PYTHONPATH
export PATH=${LBCAL_DIR}/lotss-hba-survey:${LINC_INSTALL_DIR}/scripts:$PATH
export LINC_DATA_DIR=/beegfs/car/mjh/lb

## define the data directories
DATADIR=${LINC_DATA_DIR}/${OBSID}
PROCDIR=${LINC_DATA_DIR}/processing
OUTDIR=${PROCDIR}/${OBSID}
TMPDIR=/scratch/mjh/${OBSID}/
LOGSDIR=${OUTDIR}/logs
mkdir -p ${TMPDIR}
mkdir -p ${LOGSDIR}

## go to working directory
cd ${OUTDIR}

python ${FLOCS_DIR}/runners/create_ms_list.py --filter_baselines="*&" ${DATADIR}

echo LINC starting
time cwltool --parallel --preserve-entire-environment --no-container --tmpdir-prefix=${TMPDIR} --outdir=${OUTDIR} --log-dir=${LOGSDIR} ${LINC_DATA_ROOT}/workflows/HBA_calibrator.cwl mslist.json 2>&1 | tee ${OUTDIR}/job_output.txt
echo LINC ended

if grep 'Final process status is success' ${OUTDIR}/job_output.txt
then 
	echo 'SUCCESS: Pipeline finished successfully' > ${OUTDIR}/finished.txt
else
	echo "**FAILURE**: Pipeline failed with exit status: ${?}" > ${OUTDIR}/finished.txt
fi
