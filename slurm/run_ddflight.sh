#!/bin/bash 
#SBATCH --job-name=test_ddf
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=54
#SBATCH --exclusive 
#SBATCH --time=72:00:00
#SBATCH -A durham
#SBATCH -p cosma8-shm
#SBATCH --output=/cosma8/data/do011/dc-mora2/logs/ddf-%x.%j.out

## submit the job with OBSID as an argument
OBSID=${1}

echo "Starting up, field is " ${OBSID}
hostname
cd ${DATA_DIR}/${OBSID}/ddfpipeline
cp ${DDF_PIPELINE_INSTALL}/examples/tier1-rerun.cfg .
sed -i "s~\\\$\\\$~${BOOTSTRAP_DIR}~g" tier1-rerun.cfg

singularity exec -B ${PWD},${BOOTSTRAP_DIR} ${DDFPIPELINE_SINGULARITY} CleanSHM.py
singularity exec -B ${PWD},${BOOTSTRAP_DIR} ${DDFPIPELINE_SINGULARITY} make_mslists.py
singularity exec -B ${PWD},${BOOTSTRAP_DIR} ${DDFPIPELINE_SINGULARITY} pipeline.py tier1-rerun.cfg

if test -f image_full_ampphase_di_m.NS.app.restored.fits
then
	echo "SUCCESS: Pipeline finished successfully" > finished.txt
	## move solutions etc to ../ddfsolutions
	if ! test -d ../ddfsolutions
	then
		mkdir ../ddfsolutions
	fi
	mv SOLSDIR ../ddfsolutions
	## check whether the bootstrap will need to be applied
	FILES=(logs/KillMS-L*DIS2_full.log)
	TMP=`grep ' InCol' ${FILES[1]}`
	INCOL=`echo ${TMP} | cut -d' ' -f 7`
	if [ "${INCOL}" = "DATA" ]
	then
		echo "used bootstrapped model, no corrections necessary"
	else
		## will also need     
		#L*frequencies.txt (can be reconstructed if it doesn't exist)
		#L*crossmatch-results-2.npy
		cp L*frequencies.txt ../ddfsolutions
		cp L*crossmatch-results-2.npy ../ddfsolutions
	fi
else
	echo "Pipeline did not report finishing successfully. Please check processing" > finished.txt
fi

## copy files over to the processing directory where the monitor script checks for them
## and make sure that there will be the right things to check
if ! test -d ${DATA_DIR}/processing/${OBSID}
then
	mkdir ${DATA_DIR}/processing/${OBSID}
fi
cp finished.txt ${DATA_DIR}/processing/${OBSID}/
echo -e "ddflight.cwl Resolved \n\n\n\n\n\n\n\n\n\n" > ${DATA_DIR}/processing/${OBSID}/job_output.txt
cp big-mslist.txt ${DATA_DIR}/processing/${OBSID}/mslist-ddflight.json
sed -i 's~ms~ path~g' ${DATA_DIR}/processing/${OBSID}/mslist-ddflight.json
