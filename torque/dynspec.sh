#!/bin/bash

unset PYTHONPATH
export DDF_PIPELINE_CLUSTER=Herts
echo About to run singularity, field is $FIELD
/soft/singularity-3.8.4/bin/singularity exec -B/soft -B$PWD /home/tasse/DDFSingularity/ddf.sif CleanSHM.py
/soft/singularity-3.8.4/bin/singularity exec -B/soft -B$PWD /home/tasse/DDFSingularity/ddf.sif /home/mjh/git/ddf-pipeline/scripts/run_full_field_reprocessing_pipeline.py --Dynspec --Parset=/parset/tier1-minimal.cfg --Field=$FIELD
