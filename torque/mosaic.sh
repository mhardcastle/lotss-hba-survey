#!/bin/bash

#!/bin/bash

source /home/mjh/pipeline-master/init.sh
cd /data/lofar/DR2/mosaics
mkdir $FIELD
cd $FIELD
mosaic_pointing.py --directories=/data/lofar/DR2/fields $FIELD
