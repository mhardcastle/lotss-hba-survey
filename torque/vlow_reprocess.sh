#!/bin/bash

source /home/mjh/pipeline-master/init.sh
source $LOFARSOFT
cd /beegfs/car/mjh/reprocess
run_smooth_again.py --field $FIELD

