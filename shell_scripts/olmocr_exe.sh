#!/bin/bash
#SBATCH --job-name=olmocr_extract_papers
#SBATCH --partition=gpu-h100,gpu-a100
#SBATCH --nodes=1
#SBATCH --gres=gpu:1              
#SBATCH --cpus-per-task=5        
#SBATCH --mem=80G                  # Total system RAM
#SBATCH --time=24:00:00           # Adjust time as needed
#SBATCH --output=/work/robust_ai_lab/shared/logs/sbatch_logs/olmocr_pdf_extract_%j.out
#SBATCH --error=/work/robust_ai_lab/shared/logs/sbatch_logs/olmocr_pdf_extract_%j.err

set -euo pipefail
echo "Loading env vars"
source /work/robust_ai_lab/shared/env_vars/rheum_project/env_vars.sh

#load env vars here 


export INPUT_DIR="$PDF_INPUT_PATH" # set input path here
export OUT_DIR="${PDF_OUT_PATH}_$(date +%s)" #set output path var here
export CONDA_SH="$CONDA_RHEUM_VENV"
export OLMOCR_CLIENT="$OLMOCR_SCRIPT_PATH"
export PORT="$PORT_NUMBER"

mkdir -p "${OUT_DIR}"


echo "Loading modules"

module --force purge
module load cuda/12.6
module load gcc/13.3


: "${env_vars:?set in env_vars.sh}"
echo "Activating venv"
source "${CONDA_RHEUM_VENV}"


echo "[DBG] Host: $(hostname)"
echo "[DBG] SLURM_JOB_ID=${SLURM_JOB_ID:-unset} \
SLURM_GPUS=${SLURM_GPUS:-unset} \
SLURM_JOB_GPUS=${SLURM_JOB_GPUS:-unset} \
SLURM_STEP_GPUS=${SLURM_STEP_GPUS:-unset} \
CUDA_VISIBLE_DEVICES=${CUDA_VISIBLE_DEVICES:-unset}"


srun -u --ntasks=1 --gpus=1 python - <<'PY'
import torch; print("cuda?", torch.cuda.is_available(), "count", torch.cuda.device_count())
if torch.cuda.is_available(): print(torch.cuda.get_device_name(0))
PY


echo "Crucial Libararies Available, GPU Node is Visible"

echo "[INFO] Starting Paper Extraction Process"

srun -u --ntasks=1 --gres=gpu:1 \
  python "${OLMOCR_SCRIPT_PATH}"\
  --model-name allenai/olmOCR-7B-0725-FP8 \
  --port "${PORT}" \
  --max-model-len 24576 \
  --gpu-memory-utilization 0.95 \
  --out-dir "${OUT_DIR}" \
  --markdown \
  --input-dir "${INPUT_DIR}" \
  --recursive
