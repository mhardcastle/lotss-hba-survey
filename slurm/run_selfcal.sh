#!/bin/bash 
#SBATCH -N 1                  # number of nodes
#SBATCH -c 32                 # number of cores
#SBATCH --ntasks=1            # number of tasks
#SBATCH -t 6:00:00           # maximum run time in [HH:MM:SS] or [MM:SS] or [minutes]

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
export BINDPATHS=${SOFTWAREDIR},${DATA_DIR}

#################################################################################
## IN GENERAL DO NOT TOUCH ANYTHING BELOW HERE

## define the data directories
DATADIR=${DATA_DIR}/${OBSID}/split-directions-toil
PROCDIR=${DATA_DIR}/processing
OUTDIR=${PROCDIR}/${OBSID}_${SLURM_ARRAY_TASK_ID}
LOGSDIR=${OUTDIR}/logs

TARGETINMS=`sed -n "${SLURM_ARRAY_TASK_ID}p" ${DATADIR}/targetlist.txt`

mkdir -p ${OUTDIR}
mv ${TARGETINMS} ${OUTDIR}

cd ${OUTDIR}
TARGETMS=`ls -d ILTJ*`

apptainer exec -B ${PWD},${BINDPATHS} --no-home ${LOFAR_SINGULARITY} python3 ${FACETSELFCAL}/facetselfcal.py ${TARGETMS} --helperscriptspath ${FACETSELFCAL} --helperscriptspathh5merge ${LOFARHELPERS} --configpath ${VLBIDIR}/target_selfcal_config.txt --targetcalILT=tec --ncpu-max-DP3solve=56 > facet_selfcal.log 2>&1

## check if it finishes 
if compgen -G "merged_selfcalcyle009*h5" > /dev/null; then
	## clean up and write a finished.txt
	mkdir tmp
	mv * tmp/
	mv tmp/${TARGETMS} .
	mv tmp/${TARGETMS}.copy .
	mv tmp/merged*selfcalcyle009*.h5 .
	mv tmp/plotlosoto* .
	mv tmp/*png .
	mv tmp/*MFS-image.fits .
	mv tmp/facet_selfcal.log .
	mv tmp/selfcal.log .
	rm -r tmp
	ILTJ=`ls -d ILTJ*ms | cut -d'_' -f 1`
	mkdir ${ILTJ}
	mv * ${ILTJ}/
	echo 'SUCCESS: Pipeline finished successfully' > ${OUTDIR}/finished.txt
	echo 'Resolved /fake/workflow/selfcal.cwl' > ${OUTDIR}/job_output.txt
else
        echo "**FAILURE**: Pipeline failed" > ${OUTDIR}/finished.txt
	echo 'Resolved /fake/workflow/selfcal.cwl' > ${OUTDIR}/job_output.txt
fi

