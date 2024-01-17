#!/bin/bash

# To be run inside the singularity!
# This sets env variables and then runs the main script

export LBCAL_DIR=/home/mjh/pipeline-lbcal
export LINC_INSTALL_DIR=/home/mjh/pipeline-lbcal/LINC
export LINC_DATA_ROOT=$LINC_INSTALL_DIR

export PYTHONPATH=${LINC_INSTALL_DIR}/scripts:$PYTHONPATH
export PATH=${LBCAL_DIR}/lotss-hba-survey:${LINC_INSTALL_DIR}/scripts:$PATH
export LINC_DATA_DIR=/beegfs/car/mjh/lb

run_linc_calibrator.sh $1

