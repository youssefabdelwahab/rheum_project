#!/bin/bash
#SBATCH --job-name=olmocr_extract_papers
#SBATCH --partition=gpu-a100,gpu-h100
#SBATCH --nodes=1
#SBATCH --gres=gpu:1              
#SBATCH --cpus-per-task=5        
#SBATCH --mem=80G                  # Total system RAM
#SBATCH --time=24:00:00           # Adjust time as needed

set -euo pipefail



#defualt variables
PORT="5001"
MAX_MODEL_LEN="24576"
MAX_NUM_SEQ="32"
GPU_UTIL="0.95"
MODEL_NAME="allenai/olmOCR-2-7B-1025-FP8"


# --- Argument Parsing ---
EXTRA_ARGS=()

while [[ $# -gt 0 ]]; do
  case $1 in
    --conda_venv)
      CONDA_VENV="$2"
      shift 2
      ;;
    --olmocr_client)
      OLMOCR_CLIENT="$2"
      shift 2
      ;;
    --input-dir)
      INPUT_DIR="$2"
      shift 2
      ;;
    --out-dir)
      OUT_DIR="$2"
      shift 2
      ;;
    --model-name)
      MODEL_NAME="$2"
      shift 2
      ;;
    --port)
      PORT="$2"
      shift 2
      ;;
    --max-model-len)
      MAX_MODEL_LEN="$2"
      shift 2
      ;;
    --max-num-seq)
      MAX_NUM_SEQ="$2"
      shift 2
      ;;
    *)
      EXTRA_ARGS+=("$1")
      shift
      ;;
  esac
done


# --- User Required Inputs ---
# Check if INPUT_DIR is empty/unset
if [[ -z "${INPUT_DIR:-}" ]]; then
    echo "[ERROR] You must specify an input directory using --input-dir" >&2
    exit 1
fi

# Check if OLMOCR_CLIENT is empty/unset
if [[ -z "${OLMOCR_CLIENT:-}" ]]; then
    echo "[ERROR] You must specify the client script using --olmocr_client" >&2
    exit 1
fi

if [[ -z "${CONDA_VENV:-}" ]]; then
    echo "[ERROR] You must specify the conda venv using --conda_venv" >&2
    exit 1
fi




echo "Loading modules"
module --force purge
module load cuda/12.6
module load gcc/13.3


echo "Activating venv"
source "${CONDA_VENV}"
conda activate olmocr || { echo "Conda activate failed"; exit 1; }



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
  python "${OLMOCR_CLIENT}"\
  --model-name "${MODEL_NAME}" \
  --port "${PORT}" \
  --max-model-len "${MAX_MODEL_LEN}" \
  --gpu-memory-utilization "${GPU_UTIL}" \
  --max-num-seq "${MAX_NUM_SEQ}" \
  --out-dir "${OUT_DIR}" \
  --markdown \
  --input-dir "${INPUT_DIR}" \
  --recursive \
  "${EXTRA_ARGS[@]:-}"