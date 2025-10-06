import os, sys, json, torch, socket, shutil, threading, queue, gc, threading
from pathlib import Path
from glob import glob
from datetime import datetime


resize_lock = threading.RLock()
print("cuda_is_available:", torch.cuda.is_available())
print("device_count:", torch.cuda.device_count())
print("CUDA_VISIBLE_DEVICES:", os.environ.get("CUDA_VISIBLE_DEVICES"))

print("HOST:", socket.gethostname())
print("CUDA_VISIBLE_DEVICES:", os.environ.get("CUDA_VISIBLE_DEVICES"))
print("Torch CUDA build:", getattr(__import__("torch").version, "cuda", None))
print("nvidia-smi:", shutil.which("nvidia-smi"))

from glob import glob
print(glob(os.path.join(os.environ["TORCH_EXTENSIONS_DIR"], "**", "exllamav2_ext", "*.so"), recursive=True))
print('Torch Ext File Visible')

import exllamav2
from exllamav2 import ext
from exllamav2 import ExLlamaV2, ExLlamaV2Tokenizer , ExLlamaV2Config , ExLlamaV2Cache 
from exllamav2.generator import ExLlamaV2DynamicGenerator , ExLlamaV2Sampler 
print("Exllamav2 Loaded Successfully from:", ext.__file__)

from dotenv import load_dotenv

env_path = os.getenv("SCRIPT_ENV_FILE")  # export SCRIPT_ENV_FILE=/full/path/to/env_vars.sh
if not env_path:
    raise RuntimeError("SCRIPT_ENV_FILE is not set")

env_path = str(Path(env_path).expanduser())
ok = load_dotenv(dotenv_path=env_path, override=False)
if not ok:
    raise FileNotFoundError(f"Could not load env file at {env_path}")
print("Loaded Env File")



rheum_project_dir = os.getenv("RHEUM_PROJ_DIR")
paper_database = os.getenv("PAPER_DATABASE_PATH")
llama_70B = os.getenv("MODEL_PATH_70B")
os.makedirs(paper_database, exist_ok=True)


from LLM_Agent.inference_template import  make_llama3_chat , truncate_each_to_limit_wordwise , inline_llm_call



model_path = llama_70B
MAX_CTX = 65536
MAX_NEW = 128
SAFE_HEADROOM = 64  # to be safe for stop tokens etc.
MAX_PROMPT_TOKENS = MAX_CTX - MAX_NEW - SAFE_HEADROOM

annotated_q = queue.Queue(maxsize=50)   # backpressure
SENTINEL = object()

def writer_thread(path: Path, q: queue.Queue):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path ,'a', encoding='utf-8') as f:
        while True:
            item = q.get()
            try: 
                if item is SENTINEL: 
                    break
                if item is None: 
                    continue
                batch = item if isinstance(item, (list, tuple)) else [item]   

                for rec in batch: 
                    if not rec: 
                        continue

                    if isinstance(rec, dict): 
                        doi = rec.get("doi")
                        completion = rec.get("trials")
                        rec = {"doi": doi, "trials": completion}
                    else: 
                        continue
                    f.write(json.dumps(rec, ensure_ascii=False) + "\n")
                    f.flush()
  
            finally: 
                q.task_done()

def ensure_prompt_fits(user_text: str) -> str:
    # quick iterative shrink to fit the token budget
    txt = user_text
    for _ in range(4):
        prompt = make_llama3_chat(txt)
        ids = tokenizer.encode(prompt, add_bos=True, encode_special_tokens=True)
        if len(ids) <= MAX_PROMPT_TOKENS:
            return txt
        # shrink proportionally and trim at word boundary
        ratio = MAX_PROMPT_TOKENS / max(len(ids), 1)
        target_chars = max(1000, int(len(txt) * ratio * 0.95))
        cut = txt[:target_chars]
        sp = cut.rfind(" ")
        txt = cut if sp < 0 else cut[:sp]
    return txt




probe_len = 8192
config = ExLlamaV2Config(model_path)
config.arch_compat_overrides()
config.max_input_len = probe_len
config.max_attention_size = max(getattr(config, "max_attention_size", 0), 8192 * 8192)
tokenizer = ExLlamaV2Tokenizer(config)
model = ExLlamaV2(config)
probe_cache = ExLlamaV2Cache(model, 
                            max_seq_len= probe_len, 
                            lazy=True)
model.load_autosplit(probe_cache, progress=False)
del probe_cache
torch.cuda.synchronize()
gc.collect()
torch.cuda.empty_cache()

big_len = 65536
big_cache = ExLlamaV2Cache(model, max_seq_len=big_len, lazy=False)

sampler = ExLlamaV2Sampler.Settings()
sampler.temperature = 0.0
sampler.top_p = 1.0
sampler.top_k = 0
sampler.token_healing = False
sampler.stop_on_eos = True
sampler.token_repetition_penalty_max = 1.15
sampler.token_repetition_penalty_sustain = 256
sampler.token_repetition_penalty_decay = 128
generator = ExLlamaV2DynamicGenerator(
            model=model,
            cache = big_cache, 
            tokenizer=tokenizer,
            sampler = sampler, 
            max_chunk_size = 4096,
            paged = True,
        )
generator.warmup()

print("Model Loaded")

input_path = os.path.join(paper_database, "extracted/run_1/extracted_paper_info_thread.jsonl")
out_path = os.path.join(paper_database, f"extracted/run_1/annotated_papers_{datetime.now():%Y-%m-%d_%H-%M}.jsonl")

t = threading.Thread(target=writer_thread, args=(out_path, annotated_q), daemon=True)
t.start()

list_of_papers = []
with open(input_path, 'r', encoding='utf-8') as f:
    for line in f: 
        row = json.loads(line)
        paper_info = {key: row.get(key) for key in ['doi', 'paper_text']}
        list_of_papers.append(paper_info)

print("Loaded Papers")
all_papers = [paper['paper_text'] for paper in list_of_papers]
all_paper_ids = [paper['doi'] for paper in list_of_papers]

# test_papers = all_papers[:4]
# test_papers_ids = all_paper_ids[:4]

print("Starting Annotation")
for paper_id, paper in zip(all_papers , all_paper_ids): 
    limited_paper = truncate_each_to_limit_wordwise([paper],80000)[0]
   
    ensured_paper = ensure_prompt_fits(limited_paper)
    print(f"Annotating {paper_id} ")
    records = inline_llm_call(generator, tokenizer,  sampler, ensured_paper , paper_id)
    print(f"Successfully Annotated {paper_id}")

    annotated_q.put(records)
    
# #shutdown
annotated_q.put(SENTINEL)
annotated_q.join()
t.join()
print("Annotation of Papers is Complete")
