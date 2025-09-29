#!/bin/bash
#SBATCH --job-name=annotate_medical_papers_test
#SBATCH --partition=gpu-h100            
#SBATCH --gpus=1                   # 1x H100
#SBATCH --cpus-per-task=1         # CPU threads for dataloading/tokenizer
#SBATCH --mem=64G                 # host RAM (adjust to your IO/tokenizer needs)
#SBATCH --time=00:30:00
#SBATCH --output=logs/annotation_inference_test/%x-%j.out
#SBATCH --error=logs/annotation_inference_test/%x-%j.err

echo "INFO: Purging modules and loading CUDA..."


module purge 
module load cuda/12.4
module load gcc/13.3.0

echo "INFO: Activating Python virtual environment..."


source /work/robust_ai_lab/shared/venvs/rheum_env311/bin/activate

echo "INFO: Exporting environment variables for PyTorch..."

export CUDA_HOME="/global/software/cuda/12.4.1"
export CPATH=${PYTHON_HEADERS_PATH}${CPATH:+:${CPATH}}

export TORCH_EXTENSIONS_DIR="/work/robust_ai_lab/shared/.cache/torch_ext"
export TORCH_CUDA_ARCH_LIST="9.0"

echo "-------------------- DEBUGGING INFO --------------------"
echo "INFO: SLURM_JOB_ID: $SLURM_JOB_ID"
echo "INFO: Python executable: $(which python)"
echo "INFO: CUDA_HOME is: $CUDA_HOME" # Check what 'module load' set
echo "INFO: TORCH_EXTENSIONS_DIR is: $TORCH_EXTENSIONS_DIR"
echo "INFO: CPATH is: $CPATH"
echo "--------------------- DEBUGGING INFO --------------------"


python /work/robust_ai_lab/rheum_project/testing/test_annotate.py

echo "INFO: Job finished."