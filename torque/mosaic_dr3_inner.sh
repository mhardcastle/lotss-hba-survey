#!/bin/bash

mosaic_pointing.py --directories=/beegfs/lofar/DR3/fields --ignore_field=dr3 --do-lowres --do_scaling --save-header --apply-shift $FIELD
