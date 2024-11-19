#!/bin/bash
singularity run -B/data,/beegfs /beegfs/car/mjh/DDF-v0.8.0/ddf-new.sif rerun_offsets.py

