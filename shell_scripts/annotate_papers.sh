#!/bin/bash
#SBATCH --job-name=annotate_medical_papers
#SBATCH --partition=gpu-h100,gpu-a100
#SBATCH --gpus=1                   # 1x GPU
#SBATCH --cpus-per-task=1          # CPU threads for dataloading/tokenizer
#SBATCH --mem=80G                  # host RAM (adjust to your IO/tokenizer needs)
#SBATCH --time=24:00:00


set -Eeuo pipefail

EXTRA_ARGS=()

EXTRA_ARGS=()

while [[ $# -gt 0 ]]; do
  case $1 in
    --venv)
      VENV_PATH="$2"
      shift 2
      ;;
    --script-path)
      SCRIPT_PATH="$2"
      shift 2
      ;;
    --model-path)
      MODEL_PATH="$2"
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
    --ext-dir)
      EXT_DIR="$2"
      shift 2
      ;;
    *)
      EXTRA_ARGS+=("$1")
      shift
      ;;
  esac
done

if [[ -z "${VENV_PATH:-}" ]]; then
    echo "[ERROR] You must specify the python virtual environment using --venv" >&2
    exit 1
fi

if [[ -z "${SCRIPT_PATH:-}" ]]; then
    echo "[ERROR] You must specify the python script path using --script-path" >&2
    exit 1
fi

if [[ -z "${MODEL_PATH:-}" ]]; then
    echo "[ERROR] You must specify the model path using --model-path" >&2
    exit 1
fi

if [[ -z "${INPUT_DIR:-}" ]]; then
    echo "[ERROR] You must specify an input directory using --input-dir" >&2
    exit 1
fi

if [[ -z "${OUT_DIR:-}" ]]; then
    echo "[ERROR] You must specify an output directory using --out-dir" >&2
    exit 1
fi

if [[ -z "${EXT_DIR:-}" ]]; then
    echo "[ERROR] You must specify the torch extensions directory using --ext-dir" >&2
    exit 1
fi

echo "INFO: Purging modules and loading CUDA..."
module purge 
module load cuda/12.4
module load gcc/13.3.0

echo "INFO: Activating Python virtual environment..."
source "${VENV_PATH}/bin/activate"

export TORCH_EXTENSIONS_DIR="${EXT_DIR}"

# Force-remove the lock file from your shared directory so the job doesn't hang
echo "INFO: Clearing potential zombie locks..."
# Note: You may want to parameterize this cache dir in the future if you move off this specific cluster
rm -f /work/robust_ai_lab/shared/.cache/torch_ext/lock 
rm -f /work/robust_ai_lab/shared/.cache/torch_ext/exllamav2_ext/lock

echo "-------------------- SYSTEM & CUDA DIAGNOSTICS --------------------"
echo "INFO: SLURM_JOB_ID: ${SLURM_JOB_ID:-Not Set}"
echo "INFO: HOST: $(hostname)"
echo "INFO: CUDA_VISIBLE_DEVICES: ${CUDA_VISIBLE_DEVICES:-Not Set}"
echo "INFO: nvidia-smi path: $(which nvidia-smi || echo 'Not Found')"
echo "INFO: Python executable: $(which python)"
echo "INFO: CUDA_HOME is: ${CUDA_HOME:-Not Set}"
echo "INFO: TORCH_EXTENSIONS_DIR is: ${TORCH_EXTENSIONS_DIR}"
echo "INFO: CPATH is: ${CPATH:-Not Set}"

# Inline Python for PyTorch-specific checks
python -c '
import torch
print(f"INFO: cuda_is_available: {torch.cuda.is_available()}")
print(f"INFO: device_count: {torch.cuda.device_count()}")
print(f"INFO: Torch CUDA build: {getattr(torch.version, \"cuda\", None)}")
'
echo "INFO: Starting Medical Paper Annotation Pipeline..."

srun -u --ntasks=1 --gres=gpu:1 \
  python "${SCRIPT_PATH}" \
  --model-path "${MODEL_PATH}" \
  --input-dir "${INPUT_DIR}" \
  --out-dir "${OUT_DIR}" \
  "${EXTRA_ARGS[@]:-}"

echo "INFO: Job finished."