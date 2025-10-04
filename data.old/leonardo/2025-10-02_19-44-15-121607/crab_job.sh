#!/bin/bash

#SBATCH --job-name=crab_stress_tes
#SBATCH --output=./data/leonardo/2025-10-02_19-44-15-121607/slurm_output.log
#SBATCH --error=./data/leonardo/2025-10-02_19-44-15-121607/slurm_error.log
#SBATCH --nodes=4
#SBATCH --ntasks-per-node=1
#SBATCH --partition=boost_usr_prod
#SBATCH --gres=tmpfs:0
#SBATCH --time=01:00:00

module purge
module load openmpi

echo "--- [SBATCH] Testing srun with a minimal Python script..."
srun /leonardo/home/userexternal/mmarcel3/CRAB/.venv/bin/python -u -c "import os, sys; print(f'Hello from Python on node {os.uname().nodename}, job step {os.environ.get(\'SLURM_STEP_ID\')}'); sys.stdout.flush()"
echo "--- [SBATCH] srun with minimal Python finished."
