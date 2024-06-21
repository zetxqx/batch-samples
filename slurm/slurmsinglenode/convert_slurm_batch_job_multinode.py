import os
import json
import yaml
import sys
from slurm_job_config import SlurmJobConfig, parse_slurm_script


def generate_gres_conf_script(slurm_config: SlurmJobConfig):
    gres_conf_script = f"""#!/bin/bash

# Script to configure Slurm's GPU resources in gres.conf

cat <<EOF > /usr/local/etc/slurm/gres.conf
# Define GPU resources
AutoDetect=nvml
"""
    if slurm_config.gpu_per_node > 0 and slurm_config.gpu_type not in ["None", None, ""]:
        for i in range(slurm_config.gpu_per_node):
            gres_conf_script += f"Name=gpu Type={slurm_config.gpu_type} File=/dev/nvidia{i}\n"
    else:
        for i in range(slurm_config.gpu_per_node):
            gres_conf_script += f"Name=gpu File=/dev/nvidia{i}\n"

    gres_conf_script += "EOF\n"
    return gres_conf_script

def generate_slurm_conf_script(slurm_config: SlurmJobConfig) -> str:
    node_count = slurm_config.node_count

    slurm_conf_script_fixed = """

cat <<EOF > /usr/local/etc/slurm/slurm.conf
ClusterName=${BATCH_JOB_ID}
SlurmctldHost=$(head -1 ${BATCH_HOSTS_FILE})
AuthType=auth/munge

ProctrackType=proctrack/pgid
ReturnToService=2

# For GPU resource
GresTypes=gpu

SlurmctldPidFile=/var/run/slurm/slurmctld.pid
SlurmdPidFile=/var/run/slurm/slurmd.pid
# slurm logs
SlurmdLogFile=/var/log/slurm/slurmd.log
SlurmctldLogFile=/var/log/slurm/slurmctld.log
SlurmdSpoolDir=/var/spool/slurmd

SlurmUser=root
StateSaveLocation=/var/spool/slurmctld
TaskPlugin=task/none
SchedulerType=sched/backfill
SelectTypeParameters=CR_Core

# Turn off both types of accounting
JobAcctGatherFrequency=0
JobAcctGatherType=jobacct_gather/none
AccountingStorageType=accounting_storage/none

SlurmctldDebug=3
SlurmdDebug=3
SelectType=select/cons_tres
"""

    slurm_conf_script_not_fixed = f"MaxNodeCount={node_count}\nPartitionName=all  Nodes=ALL Default=yes\nEOF"
    return slurm_conf_script_fixed + slurm_conf_script_not_fixed


def start_slurm(slurm_config: SlurmJobConfig) -> str: 
    gpu_per_node = slurm_config.gpu_per_node
    return f"""

mkdir -p /var/spool/slurm
chmod 755 /var/spool/slurm/
touch /var/log/slurmctld.log
mkdir -p /var/log/slurm
touch /var/log/slurm/slurmd.log /var/log/slurm/slurmctld.log
touch /var/log/slurm_jobacct.log /var/log/slurm_jobcomp.log

rm -rf /var/spool/slurmctld/*
if grep -qFx $(/bin/hostname) <(head -1 $BATCH_HOSTS_FILE); then
    systemctl start slurmctld
    MAX_RETRIES=5
    RETRY_INTERVAL=5
    for (( i=1; i<=MAX_RETRIES; i++ )); do
        if systemctl is-active --quiet slurmctld; then
        echo "slurmctld are running."
        break
        fi
        echo "Services not running. Retrying in $RETRY_INTERVAL seconds..."
        sleep $RETRY_INTERVAL
    done
fi
/usr/local/sbin/slurmd -Z --conf "Gres=gpu:{gpu_per_node}"
echo "printing slurmd.log"
less -5 /var/log/slurm/slurmd.log
echo "slurmd is running"
"""

def createJobJSON(slurm_conf: SlurmJobConfig) -> dict:
    slurm_setup = generate_gres_conf_script(slurm_conf) + generate_slurm_conf_script(slurm_conf) + start_slurm(slurm_conf)
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
                                "text": "if grep -qFx $(/bin/hostname) <(head -1 $BATCH_HOSTS_FILE); then\n  sleep 1800\nfi"
                            }
                        }
                    ],
                },
                "task_count": slurm_conf.node_count
            }
        ],
        "allocation_policy": {
            "location": {
                "allowed_locations": ["zones/us-central1-a"]
            },
            "instances": {
                "policy": {
                    "accelerators": {
                        "type": "nvidia-tesla-v100",
                        "count": slurm_conf.gpu_per_node
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
    output_dir = "./output-multinode"
    os.makedirs(output_dir, exist_ok=True)
    
    config = parse_slurm_script(slurm_script_path, True)
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