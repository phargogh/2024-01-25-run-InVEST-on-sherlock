# objective: stdlib only

import argparse
import datetime
import getpass
import logging
import os
import shutil
import subprocess
import sys
import time

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(os.path.basename(__file__))

NOW = datetime.datetime.now().strftime('%Y-%m-%d--%H-%M-%S')
USERNAME = getpass.getuser()

SBATCH_SCRIPT = f"""#!/bin/bash
#
#SBATCH --time=2:00:00
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4
#SBATCH --mem-per-cpu=4G
#SBATCH --mail-type=ALL
#SBATCH --mail-user={USERNAME}@stanford.edu
#SBATCH --partition=hns,normal

set -x
set -e
INVEST_VERSION="$1"
INVEST_MODELNAME="$2"
SRC_DATASTACK_LOCATION="$3"
TARGET_OAK_LOCATION="$4"

# unzip the datastack archive into a new directory on SCRATCH
DATASTACK_DIR = $SCRATCH/InVEST-$INVEST_MODELNAME-inputs-{NOW}
tar -xvzf $SRC_DATASTACK_LOCATION -C $DATASTACK_DIR

# run the model
WORKSPACE_DIR="$L_SCRATCH/$INVEST_MODELNAME"
singularity run \
        docker://ghcr.io/natcap/invest:$INVEST_VERSION \
        python -m natcap.invest run "$INVEST_MODELNAME" \
        --datastack $DATASTACK_DIR/parameters.invest.json \
        --workspace $WORKSPACE_DIR

# copy the workspace over th Oak
echo "Copying workspace to $TARGET_OAK_LOCATION"
cp -rv $WORKSPACE_DIR $TARGET_OAK_LOCATION

echo "Done!"
"""


def main():
    parser = argparse.ArgumentParser()

    # job name - make it related to the model name
    # whether to use sbatch or srun

    invest_version = '3.14.1
    oak_location = '/oak/stanford/groups/gdaily/users/jadoug06/test'
    invest_modelname = 'carbon'
    src_datastack_location = '/home/jadoug06/carbon-inputs.tar.gz'

    sbatch_filename = os.path.join(os.environ['SCRATCH'],
                                   f'InVEST-{invest_modelname}-{NOW}.sbatch')
    with open(sbatch_filename, 'rw') as sbatch_file:
        sbatch_file.write(SBATCH_SCRIPT)

    LOGGER.info("Submitting batch job")
    subprocess.call(['sbatch', sbatch_filename, invest_version,
                     invest_modelname, src_datastack_location, oak_location])
    LOGGER.info(f"Check on your job status with `squeue -u {USERNAME}`")

if __name__ == '__main__':
    main()
