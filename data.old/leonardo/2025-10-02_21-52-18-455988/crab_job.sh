#!/bin/bash

#SBATCH --job-name=crab_stress_tes
#SBATCH --output=./data/leonardo/2025-10-02_21-52-18-455988/slurm_output.log
#SBATCH --error=./data/leonardo/2025-10-02_21-52-18-455988/slurm_error.log
#SBATCH --nodes=4
#SBATCH --ntasks-per-node=1
#SBATCH --partition=boost_usr_prod
#SBATCH --gres=tmpfs:0
#SBATCH --time=01:00:00

module purge
module load openmpi

source /leonardo/home/userexternal/mmarcel3/CRAB.OLD/.venv/bin/activate

/leonardo/home/userexternal/mmarcel3/CRAB/.venv/bin/python /leonardo/home/userexternal/mmarcel3/CRAB.OLD/cli.py --worker --workdir ./data/leonardo/2025-10-02_21-52-18-455988
