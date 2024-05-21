# Slurm to Google Batch Job Converter
Quickly generate Google Batch job templates from single-node Slurm scripts, optimized for GPU workloads.

## Why This Exists
While Google Batch offers flexibility, manually crafting job templates from Slurm scripts can be tedious. This script streamlines the process, particularly when you need to provision GPU resources and want the convenience of a single-node setup.

## How to Use It

### Prerequisites:
* Python 3.x
* `gcloud` CLI (Google Cloud SDK)
* Slurm script designed for one node

### Installation
```
git clone -b singlenodejson git@github.com:zetxqx/batch-samples.git
cd ./batch-samples/slurm/slurmsinglenode
pip install -r requirements.txt 
```

### Running the Script
```
python convert_slurm_batch_job.py [path/to/your/slurm_script]
```
This will create batch_job.json and batch_job.yaml in the output folder.
The two files are equivalent Batch job template. Yaml file may have better readability.

### Modifying the Generated Batch Job file

#### Fields to Modify
* `allocation_policy.instances.policy.accelerators.count`: Adjust to match the desired number of GPUs.
* `allocation_policy.instances.policy.accelerators.type`: Change to the specific GPU type required (e.g., `nvidia-tesla-v100`)
* `allocation_policy.instances.policy.boot_disk.image`: Replace with the correct project-specific image path.
* `allocation_policy.instances.policy.boot_disk.size_gb`: Set the appropriate boot disk size in gigabytes.
* `allocation_policy.location.allowed_locations`: Ensure only one zone is listed here, and it should be a zone known to have the desired GPU resources available. 
* `taskGroups[0].task_spec.runnables[-1].script.text`: Crucial Step: Replace the placeholder `sleep 1800` command with your actual Slurm script commands using `srun` or `sbatch -W` for blocking execution.

#### Fields to Leave Unchanged
* `labels`: You can add more labels, but the label `goog-batch-dynamic-workload-scheduler: 'true'` must remain as is.
* `taskGroups[0].task_count`: This should always be set to 1.
* Other fields not explicitly mentioned above should generally be left as they are, unless you have specific customization needs beyond GPU configuration and job execution.

### Submitting Your Job
```
gcloud batch jobs submit [your-job-name] --config batch_job.yaml
```
Note: Initial job startup might take some time due to GPU resource provisioning (e.g., acquiring A100s).

## Important Notes
* One Node Only: This script is specifically designed for Slurm scripts that use a single node.
* Single Zone: Ensure your Batch job configuration is set to a single zone for compatibility.
* Customization: Adapt the generated batch_job.yaml further to match your specific requirements (e.g., machine type, region).

## Limitations
* Multi-Node Unsupported: Currently doesn't handle Slurm scripts that span multiple nodes.
* Zone Flexibility: Single-zone restriction for now.