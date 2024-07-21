#!/bin/bash

export PYTHONPATH=/usr/local/src/ms_info:/usr/local/src/drawMS:/usr/local/src/DynSpecMS:/usr/local/src/killMS:/usr/local/src/DDFacet:/usr/local/src:/usr/local/src:/usr/local/src/lotss-hba-survey:/usr/local/src/lotss-query:/home/mjh/pipeline-offsetpointings/ddf-pipeline/scripts:/home/mjh/pipeline-offsetpointings/ddf-pipeline/utils:
export PATH=/usr/local/src/ms_info:/usr/local/src/drawMS:/usr/local/src/DynSpecMS:/usr/local/src/DDFacet/SkyModel:/usr/local/src/DDFacet/DDFacet:/usr/local/src/killMS/killMS:/usr/local/src/killMS/killMS:/usr/local/src/lotss-hba-survey:/home/mjh/pipeline-offsetpointings/ddf-pipeline/scripts:/usr/local/src/DynSpecMS:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

/home/mjh/pipeline-offsetpointings/ddf-pipeline/scripts/mosaic_pointing.py --directories=/beegfs/lofar/DR3/fields --ignore_field=dr3 --do-lowres --do_scaling --save-header --apply-shift $FIELD
