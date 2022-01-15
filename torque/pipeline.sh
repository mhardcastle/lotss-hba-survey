#!/bin/bash

export CFG=${CONFIG:-/home/mjh/pipeline-master/ddf-pipeline/examples/tier1-jul2018.cfg}
echo Using ddf-pipeline config $CFG
unset PYTHONPATH
export DDF_PIPELINE_CATALOGS=/beegfs/general/mjh/bootstrap
export DDF_PIPELINE_DATABASE=True
export DDF_PIPELINE_CLUSTER=Herts
export DDF_PIPELINE_LEIDENUSER=lofararchive
export RCLONE_CONFIG_DIR=/home/mjh/macaroons

/soft/singularity-3.8.4/bin/singularity run -B/soft/,/beegfs,/data /data/lofar/mjh/ddf_py3_d11_tadcp.simg pipeline.py $CFG
