#!/bin/bash

cd /beegfs/lofar/DR3/mosaics
mkdir $FIELD
cd $FIELD
singularity run -B/soft/,/beegfs,/data /beegfs/car/mjh/DDF-v0.8.0/ddf.sif /home/mjh/pipeline-master/lotss-hba-survey/torque/mosaic_dr3_inner.sh
