#!/bin/bash
#SBATCH -J hf_dl
#SBATCH -p cpu2023
#SBATCH -c 4
#SBATCH --mem=40G
#SBATCH -t 02:00:00
#SBATCH -o logs/hf_dl_%x_%j.out
#SBATCH -e logs/hf_dl_%x_%j.err

export HUGGINGFACE_HUB_TOKEN= $HUGGINGFACE_HUB_TOKEN

export HF_HUB_ENABLE_HF_TRANSFER=1
DEST=/work/robust_ai_lab/rheum_project/exl2_models/llama_nets/3_bpw/Llama-3.1-70B-Instruct-exl2
mkdir -p "$DEST"
huggingface-cli download turboderp/Llama-3.1-70B-Instruct-exl2 \
  --revision 3.0bpw \
  --include "output-*-of-*.safetensors" "model.safetensors.index.json" \
           "tokenizer*.json" "config*.json" "special_tokens_map.json" \
  --local-dir "$DEST" --local-dir-use-symlinks False 


echo "Download complete:"
ls -lh "$DEST"