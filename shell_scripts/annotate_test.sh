#!/bin/bash
#SBATCH --job-name=annotate_medical_papers_test
#SBATCH --partition=gpu-h100            
#SBATCH --gpus=1                   # 1x H100
#SBATCH --cpus-per-task=1         # CPU threads for dataloading/tokenizer
#SBATCH --mem=80G                 # host RAM (adjust to your IO/tokenizer needs)
#SBATCH --time=00:30:00
#SBATCH --output=/work/robust_ai_lab/shared/logs/sbatch_logs/annotation_inference/%x-%j.out
#SBATCH --error=/work/robust_ai_lab/shared/logs/sbatch_logs/annotation_inference/%x-%j.err

set -Eeuo pipefail


echo "INFO: Purging modules and loading CUDA..."


module purge 
module load cuda/12.4
module load gcc/13.3.0


echo "exporting env variables ...."

source "/work/robust_ai_lab/shared/env_vars/rheum_project/env_vars.sh"

echo "INFO: Activating Python virtual environment..."
source "${RHEUM_PROJECT_VENV}/bin/activate"


echo "INFO: Exporting environment variables for PyTorch..."



echo "-------------------- DEBUGGING INFO --------------------"
echo "INFO: SLURM_JOB_ID: $SLURM_JOB_ID"
echo "INFO: Python executable: $(which python)"
echo "INFO: CUDA_HOME is: $CUDA_HOME" # Check what 'module load' set
echo "INFO: TORCH_EXTENSIONS_DIR is: $TORCH_EXTENSIONS_DIR"
echo "INFO: CPATH is: $CPATH"
echo "--------------------- DEBUGGING INFO --------------------"


python /work/robust_ai_lab/rheum_project/scripts/inline_paper_annotation.py

echo "INFO: Job finished."