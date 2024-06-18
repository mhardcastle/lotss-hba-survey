#!/bin/bash -l

# Script to untar target observation

#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --job-name=untar_${2}
#SBATCH -t 12:00:00
#SBATCH -p cosma
#SBATCH -A durham
#SBATCH -o /cosma8/data/do011/dc-timm1/LoTSS-HR/surveys/R-%x.%j.out

OUTDIR=$(dirname "${1}")

cd ${OUTDIR}

tar -xvf ${1} >> ${2}_unpack.log 2>&1

