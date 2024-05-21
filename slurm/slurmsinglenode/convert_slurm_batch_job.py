import re
import os
import json
import yaml
import sys

class SlurmJobConfig:
    def __init__(self, job_name, total_cpus, total_gpus, gpu_type, total_tasks):
        self.job_name = job_name
        self.total_cpus = total_cpus
        self.total_gpus = total_gpus
        self.gpu_type = gpu_type if gpu_type else "None"
        self.total_tasks = total_tasks

    def __str__(self):
        return (f"Job Name: {self.job_name}\n"
                f"Total CPUs: {self.total_cpus}\n"
                f"Total GPUs: {self.total_gpus}\n"
                f"GPU Type: {self.gpu_type}\n"
                f"Total Tasks: {self.total_tasks}")

def parse_slurm_script(file_path):
    with open(file_path, 'r') as file:
        content = file.read()

    job_name = re.search(r'#SBATCH\s+--job-name=(\S+)', content)
    cpus_per_task = re.search(r'#SBATCH\s+--cpus-per-task=(\d+)', content)
    gpus_per_task = re.search(r'#SBATCH\s+--gpus-per-task=(\d+)', content)
    gres_match = re.search(r'#SBATCH\s+--gres=gpu:(\S+)?:(\d+)', content)
    nodes = re.search(r'#SBATCH\s+--nodes=(\d+)', content)
    tasks_per_node = re.search(r'#SBATCH\s+--ntasks-per-node=(\d+)', content)

    if not nodes or int(nodes.group(1)) != 1:
        raise ValueError("This script currently supports only one node. Please check your Slurm script.")

    job_name = job_name.group(1) if job_name else "Unknown"
    cpus_per_task = int(cpus_per_task.group(1)) if cpus_per_task else 1
    gpus_per_task = int(gpus_per_task.group(1)) if gpus_per_task else 0
    gpu_type = gres_match.group(1) if gres_match and gres_match.group(1) else "None"
    nodes = int(nodes.group(1))
    tasks_per_node = int(tasks_per_node.group(1)) if tasks_per_node else 1

    total_tasks = nodes * tasks_per_node
    total_cpus = total_tasks * cpus_per_task

    if gres_match:
        total_gpus = int(gres_match.group(2)) * nodes  # Total GPUs based on gres directive
    else:
        total_gpus = gpus_per_task * total_tasks  # Total GPUs based on gpus-per-task directive if gres is not specified

    return SlurmJobConfig(job_name, total_cpus, total_gpus, gpu_type, total_tasks)


def generate_gres_conf_script(slurm_config):
    gres_conf_script = f"""#!/bin/bash

# Script to configure Slurm's GPU resources in gres.conf

cat <<EOF > /usr/local/etc/slurm/gres.conf
# Define GPU resources
"""
    if slurm_config.total_gpus > 0 and slurm_config.gpu_type not in ["None", None, ""]:
        for i in range(slurm_config.total_gpus):
            gres_conf_script += f"Name=gpu Type={slurm_config.gpu_type} File=/dev/nvidia{i}\n"
    else:
        for i in range(slurm_config.total_gpus):
            gres_conf_script += f"Name=gpu File=/dev/nvidia{i}\n"

    gres_conf_script += "EOF\n"
    return gres_conf_script

def generate_slurm_conf_script(slurm_config):
    if slurm_config.gpu_type not in ["None", None, ""]:
        gres_string = f"gpu:{slurm_config.gpu_type}:{slurm_config.total_gpus}"
    else:
        gres_string = f"gpu:{slurm_config.total_gpus}"

    slurm_conf_script = f"""

cat <<EOF > /usr/local/etc/slurm/slurm.conf
SlurmctldHost=$(hostname)
AuthType=auth/munge
CryptoType=crypto/munge
ProctrackType=proctrack/pgid
ReturnToService=1
GresTypes=gpu
SlurmctldPidFile=/var/run/slurm/slurmctld.pid
SlurmctldPort=6817
SlurmdPidFile=/var/run/slurm/slurmd.pid
SlurmdPort=6818
SlurmdLogFile=/var/log/slurm/slurmd.log
SlurmctldLogFile=/var/log/slurm/slurmctld.log
SlurmdSpoolDir=/var/spool/slurmd
SlurmUser=root
StateSaveLocation=/var/spool/slurmctld
SwitchType=switch/none
TaskPlugin=task/none
InactiveLimit=0
KillWait=30
MinJobAge=300
SlurmctldTimeout=120
SlurmdTimeout=300
SchedulerType=sched/backfill
SelectType=select/linear
AccountingStorageType=accounting_storage/none
ClusterName=cluster
SelectType=select/cons_tres
SelectTypeParameters=CR_Core
JobAcctGatherType=jobacct_gather/linux
SlurmctldDebug=3
SlurmdDebug=3
NodeName=$(hostname) CPUs={slurm_config.total_cpus} Gres={gres_string} State=UNKNOWN
PartitionName=googlebatch Nodes=$(hostname) Default=YES MaxTime=INFINITE State=UP
EOF
"""
    return slurm_conf_script


def start_slurm():
    return f"""

mkdir -p /var/spool/slurm
chmod 755 /var/spool/slurm/
touch /var/log/slurmctld.log
mkdir -p /var/log/slurm
touch /var/log/slurm/slurmd.log /var/log/slurm/slurmctld.log
touch /var/log/slurm_jobacct.log /var/log/slurm_jobcomp.log

systemctl restart slurmd
sleep 1
systemctl restart slurmctld
sleep 2
"""

def createJobJSON(slurm_conf):
    slurm_setup = generate_gres_conf_script(slurm_conf) + generate_slurm_conf_script(slurm_conf) + start_slurm()
    job_definition = {
        "taskGroups": [
            {
                "task_spec": {
                    "runnables": [
                        {
                            "script": {
                                "text": slurm_setup,
                            },
                        },
                        {
                            "script": {
                                "text": "sleep 1800"
                            }
                        }
                    ],
                },
                "task_count": 1
            }
        ],
        "allocation_policy": {
            "location": {
                "allowed_locations": "zones/us-central1-a"
            },
            "instances": {
                "policy": {
                    "accelerators": {
                        "type": "nvidia-tesla-v100",
                        "count": slurm_conf.total_gpus
                    },
                    "boot_disk": {
                        "image": "projects/projectofbob/global/images/image-ias-test",
                        "size_gb": 50
                    }
                },
                "install_gpu_drivers": True
            }
        },
        "labels": {
            "goog-batch-dynamic-workload-scheduler": "true"
        },
        "logs_policy": {
            "destination": "CLOUD_LOGGING"
        }
    }
    return job_definition

def main():
    if len(sys.argv) != 2:
        print(
            'Usage: python3 convert_slurm_batch_job.py <slurm_script_path>'
        )
        sys.exit(1)

    slurm_script_path = sys.argv[1]
    output_dir = "./output"
    os.makedirs(output_dir, exist_ok=True)
    
    config = parse_slurm_script(slurm_script_path)
    json_data = json.dumps(createJobJSON(config), indent=4)
    json_file_path = os.path.join(output_dir, "batch_job.json")
    with open(json_file_path, 'w') as json_file:
        json_file.write(json_data)

    def str_presenter(dumper, data):
        """configures yaml for dumping multiline strings
        Ref: https://stackoverflow.com/questions/8640959/how-can-i-control-what-scalar-form-pyyaml-uses-for-my-data"""
        if data.count('\n') > 0:  # check for multiline string
            return dumper.represent_scalar('tag:yaml.org,2002:str', data, style='|')
        return dumper.represent_scalar('tag:yaml.org,2002:str', data)
    yaml.add_representer(str, str_presenter)
    yaml.representer.SafeRepresenter.add_representer(str, str_presenter)
    yaml_data = yaml.dump(createJobJSON(config), allow_unicode=True)
    yaml_file_path = os.path.join(output_dir, "batch_job.yaml")
    with open(yaml_file_path, 'w', encoding='utf-8') as file:
        file.write(yaml_data)

if __name__ == '__main__':
  main()