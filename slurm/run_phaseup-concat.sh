#!/bin/bash 
#SBATCH -N 1                  # number of nodes
#SBATCH -c 48                 # number of cores  ### CLUSTER SPECIFIC
#SBATCH --ntasks=1            # number of tasks
#SBATCH -t 72:00:00           # maximum run time in [HH:MM:SS] or [MM:SS] or [minutes]
#SBATCH -A do011
#SBATCH -p cosma8-ska2
#SBATCH -w mad03
#SBATCH --output=/cosma8/data/do011/dc-mora2/logs/R-%x.%j.out  ### CLUSTER SPECIFIC

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
singularity exec -B ${PWD},${BINDPATHS} --no-home ${LOFAR_SINGULARITY} python3 ${FLOCSDIR}/runners/create_ms_list.py VLBI phaseup-concat --delay_calibrator ${DATA_DIR}/${OBSID}/../delay_calibrators.csv --configfile ${VLBIDIR}/facetselfcal_config.txt --selfcal ${FACETSELFCAL} --h5merger ${LOFARHELPERS} --flags ${DATADIR}/flagged_fraction_antenna.json ${DATADIR}/../setup/flagged_fraction_antenna.json ${DATADIR}/../setup/flagged_fraction_antenna.json_2 --linc ${LINCDIR} --check_Ateam_separation.json ${DATADIR}/../setup/Ateam_separation.json --ms_suffix .ms ${DATADIR} >> create_ms_list.log 2>&1


#  --numbands NUMBANDS   The number of bands to group. -1 means all bands. (default: -1)
#  --firstSB FIRSTSB     If set, reference the grouping of files to this station subband. (default: None)
#  --pipeline PIPELINE   Name of the pipeline. (default: VLBI)
#  --run_type RUN_TYPE   Type of the pipeline. (default: sol000)
#  --filter_baselines FILTER_BASELINES  Selects only this set of baselines to be processed. Choose [CR]S*& if you want to process only cross-correlations and remove international stations. (default: [CR]S*&)
#  --bad_antennas BAD_ANTENNAS        Antenna string to be processed. (default: [CR]S*&)
#  --compare_stations_filter COMPARE_STATIONS_FILTER
#  --check_Ateam_separation.json CHECK_ATEAM_SEPARATION.JSON
#  --clip_sources [CLIP_SOURCES ...]
#  --removed_bands [REMOVED_BANDS ...]   The list of bands that were removed from the data. (default: [])
#  --min_unflagged_fraction MIN_UNFLAGGED_FRACTION   The minimum fraction of unflagged data per band to continue. (default: 0.5)
#  --refant REFANT       The reference antenna used. (default: CS001HBA0)
#  --max_dp3_threads MAX_DP3_THREADS   Number of threads per process for DP3. (default: 5)


echo LINC starting
TMPID=`echo ${OBSID} | cut -d'/' -f 1`
echo export PYTHONPATH=\$LINC_DATA_ROOT/scripts:\$PYTHONPATH > tmprunner_${TMPID}.sh
echo 'cwltool --parallel --preserve-entire-environment --no-container --tmpdir-prefix=${TMPDIR} --outdir=${OUTDIR} --log-dir=${LOGSDIR} ${VLBIDIR}/workflows/phaseup-concat.cwl mslist_VLBI_phaseup-concat.json' >> tmprunner_${TMPID}.sh
(time singularity exec -B ${PWD},${BINDPATHS} --no-home ${LOFAR_SINGULARITY} bash tmprunner_${TMPID}.sh 2>&1) | tee ${OUTDIR}/job_output.txt
echo LINC ended
if grep 'Final process status is success' ${OUTDIR}/job_output.txt
then 
	echo 'SUCCESS: Pipeline finished successfully' > ${OUTDIR}/finished.txt
else
	echo "**FAILURE**: Pipeline failed with exit status: ${?}" > ${OUTDIR}/finished.txt
fi

