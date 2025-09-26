#!/bin/bash
#SBATCH --job-name=flashattention_setup
#SBATCH --partition=bigmem
#SBATCH --nodes=1
#SBATCH --cpus-per-task=20
#SBATCH --mem=1800G
#SBATCH --time=4:00:00
#SBATCH --output=flashattn.out
#SBATCH --error=flashattn.err




set -euo pipefail


# --- your env
source /work/robust_ai_lab/rheum_project/env_paths.sh
: "${RHEUM_PROJECT_VENV_PATH:?set in env_paths.sh}"
source "$RHEUM_PROJECT_VENV_PATH/bin/activate"

# --- toolchain
module purge
module load cuda/12.4
module load gcc/13.3.0

python --version
python -c "from sysconfig import get_paths; print(get_paths()['include'])"

export CPATH=${PYTHON_HEADERS_PATH}${CPATH:+:${CPATH}}
export TORCH_CUDA_ARCH_LIST="90;80"

export MAX_JOBS="${SLURM_CPUS_PER_TASK:-20}"
export CMAKE_BUILD_PARALLEL_LEVEL="$MAX_JOBS"
export NVCC_THREADS=1

pip install -q --upgrade pip wheel setuptools ninja


cd /work/robust_ai_lab/rheum_project # or any desired directory
if [ ! -d "flash-attention" ]; then
  git clone https://github.com/Dao-AILab/flash-attention.git
  cd flash-attention
  git checkout v2.5.7  # Or latest compatible with exllamav2
else
  cd flash-attention
fi



rm -rf build dist flash_attn.egg-info .pip-tmp
echo "Starting FlashAttention install at $(date)"


pip wheel . -w dist --no-build-isolation
pip install dist/flash_attn-*.whl

echo "Finished install at $(date)"