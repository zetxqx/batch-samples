#!/bin/bash

#SBATCH --job-name=test                     
#SBATCH --nodes=1               		    # node count
#SBATCH --ntasks-per-node=2                 # total number of tasks across all nodes
#SBATCH --cpus-per-task=1                   # cpu-cores per task (>1 if multi-threaded tasks)
#SBATCH --gpus-per-task=1                   # gpu-cores per task (>1 if multi-threaded tasks)
#SBATCH --gres=gpu:2                        # number of gpus per node

srun -n 2 hostname
