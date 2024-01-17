#!/bin/bash -eu
#
# Script to run the LINC calibrator pipeline
#
#SBATCH -N 1                  # number of nodes
#SBATCH -c 16                 # number of cores
#SBATCH --ntasks=1            # number of tasks
#SBATCH -t 12:00:00           # maximum run time in [HH:MM:SS] or [MM:SS] or [minutes]
#SBATCH -p cosma              # partition (queue); job can run up to 3 days
#SBATCH -A durham
#SBATCH --output=R-%x.%j.out

# Error function
error()
{
  echo -e "ERROR: $@" >&2
  exit 1
}

## submit the job with this as an argument
OBSID=${1}

## define directories
LINC_WORKING_DIR=${LINC_DATA_DIR}/${OBSID}
INPUT_DIR=${LINC_DATA_DIR}/${OBSID}
OUTPUT_DIR=${LINC_WORKING_DIR}/${OBSID}
TEMP_DIR=${LINC_WORKING_DIR}/${OBSID}/tmp
mkdir -p ${TEMP_DIR}

# Tar-ball that will contain all the log files produced by the pipeline
LOGFILES=${OUTPUT_DIR}/logfiles.tar.gz

# Input file
YAMLFILE=${OUTPUT_DIR}/${OBSID}.yaml

# Define workflow
WORKFLOW=${LINC_INSTALL_DIR}/workflows/HBA_calibrator.cwl

## set up the input file 
# Environment variables that may be overridden by the user
SKYMODEL_DIR="${SKYMODEL_DIR:-${LINC_INSTALL_DIR}/skymodels}"
SKYMODEL_A_TEAM="${SKYMODEL_A_TEAM:-Ateam_LBA_CC.skymodel}"
# Check if skymodels exist
[ -d ${SKYMODEL_DIR} ] \
    || error "Skymodel directory '${SKYMODEL_DIR}' does not exist"
[ -f ${SKYMODEL_DIR}/${SKYMODEL_A_TEAM} ] \
    || error "Skymodel file '${SKYMODEL_DIR}/${SKYMODEL_A_TEAM}' not found"

# Fetch list of MS files, determine length and index of last element
declare FILES=($(ls -1d ${INPUT_DIR}/*.MS 2>/dev/null))
len=${#FILES[@]}
last=$(expr ${len} - 1)
[ ${len} -gt 0 ] || error "Directory '${INPUT_DIR}' contains no MS-files"

# Open output file
exec 3> ${YAMLFILE}

# Write file contents
cat >&3 <<EOF
msin:
$(for((i=0; i<${len}; i++))
  do
    echo "    - class: \"Directory\""
    echo "      path : \"${FILES[$i]}\""
  done
)
do_demix: false
do_smooth: false
skip_international: false
A-Team_skymodel: 
    class: "File"
    path: "${SKYMODEL_DIR}/${SKYMODEL_A_TEAM}"
calibrator_path_skymodel:
    class: "Directory"
    path: "${SKYMODEL_DIR}/"
EOF

# Close output file
exec 3>&-

echo "Wrote output to '${YAMLFILE}'"

# Increase open file limit to hardware limit
ulimit -n $(ulimit -Hn)

# Print all SLURM variables
echo -e "
================  SLURM variables  ================
$(for s in ${!SLURM@}; do echo "${s}=${!s}"; done)
===================================================
"

# Show current shell ulimits
echo -e "
============  Current resource limits  ============
$(ulimit -a)
===================================================
"

# Tell user what variables will be used:
echo -e "
The LINC pipeline will run, using the following settings:
  Input directory          : ${INPUT_DIR}
  Input specification file : ${YAMLFILE}
  Workflow definition file : ${WORKFLOW}
  Output directory         : ${OUTPUT_DIR}
  Temporary directory      : ${TEMP_DIR}
  Tar-ball of all log files: ${LOGFILES} 
"

# Check if directories and files actually exist. If not, bail out.
[ -d ${INPUT_DIR} ] || error "Directory '${INPUT_DIR}' does not exist"
[ -f ${WORKFLOW} ] || error "Workflow file '${WORKFLOW}' does not exist"
[ -f ${YAMLFILE} ] || error "Input specification file '${YAMLFILE}' does not exist"

# Adjust these to your needs
mkdir -p ${OUTPUT_DIR}/Work
# Command that will be used to run the CWL workflow
COMMAND="toil-cwl-runner  \
  --no-container \
  --stats \
  --bypass-file-store \
  --clean never \
  --cleanWorkDir never \
  --preserve-entire-environment \
  --maxCores 32 \
  --workDir ${OUTPUT_DIR}/Work \
  --jobStore ${OUTPUT_DIR}/jobStore \
  --outdir ${OUTPUT_DIR} \
  --tmpdir-prefix ${TEMP_DIR} \
  --tmp-outdir-prefix ${TEMP_DIR} \
  ${WORKFLOW} \
  ${YAMLFILE}"

echo "${COMMAND}"

# Execute command
if ${COMMAND}
then
  echo -e "\nSUCCESS: Pipeline finished successfully\n"
  exit 0
else
  STATUS=${?}
  if [ -d ${TEMP_DIR} ]
  then
    # Create sorted list of contents of ${TEMP_DIR}
    find ${TEMP_DIR} | sort > ${TEMP_DIR}/contents.log
    # Save all log files for later inspection.
    find ${TEMP_DIR} -name "*.log" -print0 | \
      tar czf ${LOGFILES} --null -T -
  fi
  echo -e "\n**FAILURE**: Pipeline failed with exit status: ${STATUS}\n"
  exit ${STATUS}
fi
       
