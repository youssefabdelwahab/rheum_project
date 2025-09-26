#!/bin/bash
#SBATCH -J build_exllamav2
#SBATCH --partition=bigmem       
#SBATCH --cpus-per-task=20
#SBATCH --mem=80G
#SBATCH -t 02:00:00
#SBATCH -o logs/build_exllama_%j.out
#SBATCH -e logs/build_exllama_%j.err

set -euo pipefail

# --- Load site toolchain (adjust to your cluster) ---
module load cuda/12.4
module load gcc/13.3.0
# module load python/3.11  # if you use a module; otherwise rely on venv/conda

# --- Use a persistent cache so future jobs never rebuild ---
export TORCH_EXTENSIONS_DIR=/work/robust_ai_lab/torch_extensions_cache
mkdir -p "$TORCH_EXTENSIONS_DIR"

export TORCH_CUDA_ARCH_LIST="9.0;8.0"   # H100 (SM90)
export MAX_JOBS="${SLURM_CPUS_PER_TASK:-20}"                   
export TORCH_CUDA_VERBOSE_BUILD=1

# (Optional) Ensure ninja is available
python -m pip install  --quiet ninja || true
export PATH="$HOME/.local/bin:$PATH"

python /work/robust_ai_lab/rheum_project/setup_folder/compile_exl.py
