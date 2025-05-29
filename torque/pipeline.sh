#!/bin/bash

export CFG=${CONFIG:-/home/mjh/pipeline-master/ddf-pipeline/examples/tier1-jul2018.cfg}
echo Using ddf-pipeline config $CFG
unset PYTHONPATH
export DDF_PIPELINE_CATALOGS=/beegfs/general/mjh/bootstrap
export DDF_PIPELINE_DATABASE=True
export DDF_PIPELINE_CLUSTER=Herts
export DDF_PIPELINE_LEIDENUSER=lofararchive
export RCLONE_CONFIG_DIR=/home/mjh/macaroons

singularity run -B/soft/,/beegfs,/data /beegfs/car/mjh/DDF-v0.8.0/ddf-new.sif pipeline.py $CFG
