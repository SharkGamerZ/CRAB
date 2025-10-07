#!/bin/bash

#SBATCH --job-name=crab_32victim_3
#SBATCH --output=./data/leonardo/2025-10-07_17-36-18-713449/slurm_output.log
#SBATCH --error=./data/leonardo/2025-10-07_17-36-18-713449/slurm_error.log
#SBATCH --nodes=64
#SBATCH --ntasks-per-node=1
#SBATCH --account=IscrB_SWING
#SBATCH --gres=tmpfs:0
#SBATCH --time=01:00:00

#SBATCH --partition=boost_usr_prod
module purge
module load openmpi

source /leonardo/home/userexternal/mmarcel3/crab/.venv/bin/activate

/leonardo/home/userexternal/mmarcel3/crab/.venv/bin/python /leonardo/home/userexternal/mmarcel3/crab/cli.py --worker --workdir ./data/leonardo/2025-10-07_17-36-18-713449
