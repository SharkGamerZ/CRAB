#!/bin/bash

#SBATCH --job-name=crab_stress_tes
#SBATCH --output=./data/leonardo/2025-10-02_19-56-17-039918/slurm_output.log
#SBATCH --error=./data/leonardo/2025-10-02_19-56-17-039918/slurm_error.log
#SBATCH --nodes=4
#SBATCH --ntasks-per-node=1
#SBATCH --partition=boost_usr_prod
#SBATCH --gres=tmpfs:0
#SBATCH --time=01:00:00

module purge
module load openmpi

echo "--- [SBATCH] Testing srun with pandas import..."
srun /leonardo/home/userexternal/mmarcel3/CRAB/.venv/bin/python -u -c "import pandas; print(f'Pandas {pandas.__version__} imported successfully')"
echo "--- [SBATCH] srun with pandas import finished."
