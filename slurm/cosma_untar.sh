#!/bin/bash -l

# Script to untar target observation

#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --job-name=untar_${2}
#SBATCH -t 12:00:00
#SBATCH --output=/dev/null 
#SBATCH --error=/dev/null
#SBATCH -p cosma
#SBATCH -A durham


tar -xvf ${1} >> ${2}_unpack.log 2>&1

