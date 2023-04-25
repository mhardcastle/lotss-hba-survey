#!/bin/bash
# Job name:
#SBATCH --job-name=test
#
# Request one node:
#SBATCH --nodes=1
#
# Specify number of tasks for use case (example):
#SBATCH --ntasks-per-node=1
#
# Processors per task:
#SBATCH --cpus-per-task=54
#
# Exclusive flag -- force each job to run on one node even though there may be cores free
#SBATCH --exclusive 
#
# Wall clock limit:
#SBATCH --time=18:00:00
#
## Command(s) to run (example):
echo "Starting up, field is " $FIELD
hostname
cd /home/azimuth/DS
export DDF_PIPELINE_CLUSTER=Azimuth
singularity exec -B$PWD /home/azimuth/ddf.sif CleanSHM.py
singularity exec -B$PWD /home/azimuth/ddf.sif run_full_field_reprocessing_pipeline.py --Dynspec --Parset=/parset/tier1-minimal.cfg --Field=$FIELD
