#!/bin/bash
cd /beegfs/lofar/DR3/fields

/soft/singularity-3.8.4/bin/singularity run -B/soft/,/beegfs,/data /beegfs/car/mjh/DDF-v0.8.0/ddf.sif /home/mjh/pipeline-offsetpointings/lotss-hba-survey/torque/make_noisemap_inner.sh
