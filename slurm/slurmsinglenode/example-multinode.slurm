#!/bin/bash

#SBATCH --job-name=test                     
#SBATCH --nodes=4               		    # node count
#SBATCH --ntasks-per-node=2                 # total number of tasks across all nodes
#SBATCH --cpus-per-task=1                   # cpu-cores per task (>1 if multi-threaded tasks)
#SBATCH --gpus-per-task=2                   # gpu-cores per task (>1 if multi-threaded tasks)

srun -n 2 hostname
