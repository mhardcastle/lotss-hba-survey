#!/bin/bash -l

# Script to dysco compress target observation

#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4
#SBATCH --job-name=dysco_${2}
#SBATCH -t 12:00:00
#SBATCH -p cosma
#SBATCH --output=/dev/null 
#SBATCH --error=/dev/null
#SBATCH -A durham

module load singularity

echo "Starting Job"

WORKING_DIR=${1}

cd ${WORKING_DIR}

SIMG=${LOFAR_SINGULARITY}

MSFILE_LIST=`sed -n ${SLURM_ARRAY_TASK_ID}p myfiles.txt`
MSFILE=$(basename ${MSFILE_LIST})
## make a parset to compress
cat >> dysco_${SLURM_ARRAY_TASK_ID}.parset << EOF
msin=${MSFILE}
msin.datacolumn=DATA
msout=${MSFILE}.dysco
msout.datacolumn=DATA
msout.storagemanager=dysco
numthreads=4
steps=[count]
EOF
echo Setup complete - Running NDPPP

singularity exec -B ${WORKING_DIR} ${SIMG} DP3 dysco_${SLURM_ARRAY_TASK_ID}.parset > dysco_${SLURM_ARRAY_TASK_ID}.log 2>&1

rm -r dysco_${SLURM_ARRAY_TASK_ID}.log dysco_${SLURM_ARRAY_TASK_ID}.parset ${MSFILE}

mv ${MSFILE}.dysco ${MSFILE}

echo Pipeline finished