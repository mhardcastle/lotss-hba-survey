#!/bin/bash

source /home/mjh/pipeline-master/init.sh
cd /data/lofar/DR2/mosaics/$FIELD
make_band_mos_cat.py $FIELD
