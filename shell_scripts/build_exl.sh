#!/bin/bash
#SBATCH -J build_exllamav2
#SBATCH --partition=bigmem       
#SBATCH --cpus-per-task=20
#SBATCH --mem=80G
#SBATCH -t 02:00:00
#SBATCH -o logs/build_exllama_%j.out
#SBATCH -e logs/build_exllama_%j.err

set -euo pipefail

source /work/robust_ai_lab/shared/env_vars/rheum_project/env_vars.sh

: "${RHEUM_PROJECT_VENV_PATH:?set in env_paths.sh}"
source "$RHEUM_PROJECT_VENV_PATH/bin/activate"

# --- ENV / modules (adjust to your site) ---
module purge
module load cuda/12.4
module load gcc/13.3.0

python --version
python -c "from sysconfig import get_paths; print(get_paths()['include'])"

export CPATH=${PYTHON_HEADERS_PATH}${CPATH:+:${CPATH}}


export CUDA_HOME="$(dirname "$(dirname "$(command -v nvcc)")")"
echo "CUDA_HOME: $CUDA_HOME"



           # or whatever your cluster uses

# If you use conda, uncomment and point to your env
# source ~/miniconda3/etc/profile.d/conda.sh
# conda activate robust_lab
# Path to the directory that CONTAINS Python.h



export TORCH_EXTENSIONS_DIR=/work/robust_ai_lab/shared/.cache/torch_ext   # shared & clean
export TORCH_CUDA_ARCH_LIST="9.0;8.0"   # H100 only. Use "8.0;9.0" to support A100+H100.
export MAX_JOBS=${SLURM_CPUS_PER_TASK:-20}
export HF_HOME=/work/robust_ai_lab/shared/.cache/hf




# Start from a totally clean extensions dir to force rebuild
mkdir -p "$TORCH_EXTENSIONS_DIR"

cd /work/robust_ai_lab/rheum_project


# Ninja makes builds much faster; install to the current env if missing
python - <<'PY'
try:
    import ninja  # noqa: F401
except Exception:
    import sys, subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "--no-input", "ninja"])
PY

# Kick off a fresh build (no reuse): we delete any cached exllama bits and import

python /work/robust_ai_lab/rheum_project/setup_folder/build_exl.py
