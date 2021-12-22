#!/bin/bash

cd $WD
unset PYTHONPATH
export DDF_PIPELINE_CATALOGS=/beegfs/general/mjh/bootstrap
export DDF_PIPELINE_DATABASE=True
export DDF_PIPELINE_CLUSTER=Herts
export DDF_PIPELINE_LEIDENUSER=lofararchive
export RCLONE_CONFIG_DIR=/home/mjh/macaroons
export RCLONE_COMMAND=/soft/bin/rclone
export ADA_COMMAND=/home/mjh/git/SpiderScripts/ada/ada

/soft/singularity-3.8.4/bin/singularity run -B/soft/,/beegfs,/data /data/lofar/mjh/ddf_py3_d11_tadcp.simg pipeline.py tier1-config.cfg
