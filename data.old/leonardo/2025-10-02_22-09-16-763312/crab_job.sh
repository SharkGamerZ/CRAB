#!/bin/bash

#SBATCH --job-name=crab_stress_tes
#SBATCH --output=./data/leonardo/2025-10-02_22-09-16-763312/slurm_output.log
#SBATCH --error=./data/leonardo/2025-10-02_22-09-16-763312/slurm_error.log
#SBATCH --nodes=4
#SBATCH --ntasks-per-node=1
#SBATCH --partition=boost_usr_prod
#SBATCH --gres=tmpfs:0
#SBATCH --time=01:00:00

module purge
module load openmpi

source /leonardo/home/userexternal/mmarcel3/CRAB.OLD/.venv/bin/activate

/leonardo/home/userexternal/mmarcel3/CRAB/.venv/bin/python /leonardo/home/userexternal/mmarcel3/CRAB.OLD/cli.py --worker --workdir ./data/leonardo/2025-10-02_22-09-16-763312
