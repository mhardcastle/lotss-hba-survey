#!/bin/bash

source /home/mjh/pipeline-master/init.sh
cd /data/lofar/DR2/mosaics/$FIELD
#mosaic_pointing.py --do_scaling --band 0 --directories=/data/lofar/DR2/fields $FIELD
#mosaic_pointing.py --do_scaling --band 1 --directories=/data/lofar/DR2/fields $FIELD
mosaic_pointing.py --do_scaling --band 2 --directories=/data/lofar/DR2/fields $FIELD
