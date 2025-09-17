#!/bin/bash
#SBATCH --job-name=annotate_medical_papers_test
#SBATCH --partition=gpu-h100            
#SBATCH --gpus=1                   # 1x H100
#SBATCH --cpus-per-task=16         # CPU threads for dataloading/tokenizer
#SBATCH --mem=120G                 # host RAM (adjust to your IO/tokenizer needs)
#SBATCH --time=1:00:00
#SBATCH --output=logs/annotation_inference_test/%x-%j.out
#SBATCH --error=logs/annotation_inference_test/%x-%j.err



module purge 
module load cuda/12.4


source /work/robust_ai_lab/rheum_project/.robust_lab/bin/activate


python /work/robust_ai_lab/rheum_project/testing/test_annotate.py