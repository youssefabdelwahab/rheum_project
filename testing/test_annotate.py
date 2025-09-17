import os, sys, json, torch , asyncio
from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime

cwd = os.getcwd()
parent_folder = os.path.abspath(os.path.join(cwd, ".."))
sys.path.append(parent_folder)


from LLM_Agent.batch_inference_temp import load_model_on_gpu, batch_call_llm
# from functions.batch_papers import load_batches
from prompts.system_prompts import get_main_clinical_trial


today_str = datetime.today().strftime('%Y/%m/%d')
repo_root = Path.cwd().parent
model = '/work/robust_ai_lab/rheum_project/exl2_models/llama_nets/3_bpw/Llama-3.1-70B-Instruct-exl2'
get_main_clinical_trial = "This is a medical research paper. Provide the name of the main trial(s) researched in the paper and their clinical trial number(s). "

annotation_dir = os.path.join(repo_root, f'research_paper_database/test_papers/annotations/{today_str}')
os.makedirs(annotation_dir, exist_ok=True)
output_path = os.path.join(annotation_dir, 'annotated_papers.jsonl')
input_path = os.path.join(repo_root, 'research_paper_database/test_papers/test_papers.jsonl')

list_of_papers = []

with open(input_path, 'r', encoding='utf-8') as f:
    for line in f: 
        row = json.loads(line)
        paper_info = {key: row.get(key) for key in ['doi', 'paper_text']}
        list_of_papers.append(paper_info)

all_papers = [paper['paper_text'] for paper in list_of_papers]
all_paper_ids = [paper['doi'] for paper in list_of_papers]


generator, sampler,  tokenizer = load_model_on_gpu(model_path=model)

records = batch_call_llm(generator, sampler, tokenizer, get_main_clinical_trial, all_papers, all_paper_ids)


with open(output_path, 'w', encoding='utf-8') as output_f:
    for record in records:
        json.dump(record , output_f , ensure_ascii=False)
        output_f.write('\n')