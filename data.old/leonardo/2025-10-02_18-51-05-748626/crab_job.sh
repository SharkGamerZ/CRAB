#!/bin/bash

#SBATCH --job-name=crab_stress_tes
#SBATCH --output=./data/leonardo/2025-10-02_18-51-05-748626/slurm_output.log
#SBATCH --error=./data/leonardo/2025-10-02_18-51-05-748626/slurm_error.log
#SBATCH --nodes=4
#SBATCH --ntasks-per-node=1
#SBATCH --partition=boost_usr_prod
#SBATCH --gres=tmpfs:0
#SBATCH --time=01:00:00

module purge
module load gcc/11.3.0
module load openmpi/4.1.4--gcc--11.3.0

srun /leonardo/home/userexternal/mmarcel3/CRAB/.venv/bin/python /leonardo/home/userexternal/mmarcel3/CRAB/cli.py --config ./data/leonardo/2025-10-02_18-51-05-748626/config.json --worker --preset leonardo
