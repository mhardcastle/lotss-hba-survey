#!/bin/bash

unset PYTHONPATH
export DDF_PIPELINE_CLUSTER=Herts
echo About to run singularity, obsdir is $OBSID
/soft/singularity-3.8.4/bin/singularity exec -B/soft -B$PWD /home/tasse/DDFSingularity/ddf.sif CleanSHM.py
/soft/singularity-3.8.4/bin/singularity exec -B/soft -B$PWD -B/local /home/tasse/DDFSingularity/ddf.sif /beegfs/lofar/deepfields/ELAIS-N1_DR2_radio/full/reprocess_obs.py $OBSID
