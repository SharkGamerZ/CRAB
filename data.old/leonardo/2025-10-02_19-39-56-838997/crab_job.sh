#!/bin/bash

#SBATCH --job-name=crab_stress_tes
#SBATCH --output=./data/leonardo/2025-10-02_19-39-56-838997/slurm_output.log
#SBATCH --error=./data/leonardo/2025-10-02_19-39-56-838997/slurm_error.log
#SBATCH --nodes=4
#SBATCH --ntasks-per-node=1
#SBATCH --partition=boost_usr_prod
#SBATCH --gres=tmpfs:0
#SBATCH --time=01:00:00

module purge
module load openmpi

echo "--- [SBATCH] Testing srun with hostname..."
srun hostname
echo "--- [SBATCH] srun hostname finished."
