# lotss-hba-survey
Contains scripts used for running the HBA component of the LoTSS survey.

# Requirements

Pip-installable Python requirements are listed in `requirements.txt`. These can be installed via

```bash
pip install -r requirements.txt
```

Other requirements are:

* https://git.astron.nl/astron-sdc/lofar_stager_api.git
* https://github.com/mhardcastle/lotss-query
* https://github.com/mhardcastle/ddf-pipeline (ddf-pipeline/utils and ddf-pipeline/scripts on PYTHONPATH)
* https://github.com/LOFAR-VLBI/lofar-vlbi-pipeline

# Environment setup
The following environment variables should be set:

* `DDF_PIPELINE_CLUSTER`: the name of the cluster you are running on, e.g. `cosma`, `spider`, `Herts` etc.
* `DATA_DIR`: directory where data will be stored while processing.
* `LOFAR_SINGULARITY`: path to an Apptainer/Singularity container with the required processing software.
* `$HOME/.surveys` with surveys database login credentials.
* `$HOME/.stagingrc` with LTA login credentials.
