import os, sys, json, torch, socket, shutil, threading, queue
from pathlib import Path
from glob import glob


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
load_dotenv(os.environ.get("script_env_file"))
print("Load Env File")



rheum_project_dir = os.environ.get("rheum_dir_path")
paper_database = os.environ.get("paper_database_path")
llama_70B = os.environ.get("model_path_70b")


from LLM_Agent.batch_inference_temp import  batch_call_llm , make_llama3_user_msg , chunk_text_by_char_limit


model_path = llama_70B
assert os.path.isdir(model_path)
if model_path not in sys.path: 
    sys.path.insert(0, model_path)

annotated_q = queue.Queue(maxsize=50)   # backpressure
SENTINEL = object()

def writer_thread(path: Path, q: queue.Queue):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('a', encoding='utf-8') as f:
        while True:
            item = q.get()
            if item is SENTINEL:
                q.task_done()
                break
            f.write(json.dumps(item, ensure_ascii=False) + "\n")
            q.task_done()

def build_cache_and_generator(model, tokenizer, max_ctx_tokens:int) : 
    max_ctx_tokens = ((max_ctx_tokens + 255) // 256) * 256  # round up to multiple of 256
    new_cache = ExLlamaV2Cache(model, max_seq_len=max_ctx_tokens, lazy=True) 
    if not getattr(cache, "loaded", False): 
        model.load_autosplit(new_cache, progress=True)
    new_generator = ExLlamaV2DynamicGenerator(
        model=model, 
        cache=new_cache, 
        tokenizer = tokenizer,
        max_chunk_size=4096,
        paged=True)
    new_generator.warmup()
    return new_generator, new_cache

def tokens_needed(tokenizer, prompt_texts:list, max_new_tokens:int): 
    longest = 0
    for t in prompt_texts: 
        prompt_message = make_llama3_user_msg(t)
       
        n = len(( tokenizer.encode(prompt_message, encode_special_tokens=True))[0])
        if n > longest: longest = n
    return longest + max_new_tokens

def ensure_capacity(model, tokenizer, total_needed_tokens:int, hardcap:int = 65536): 
    global  generator , cache
    desired = min(((total_needed_tokens + 255) // 256) * 256, hardcap)
    if desired > cache.max_seq_len: 
        generator, cache = build_cache_and_generator(model , tokenizer, desired)
  
config = ExLlamaV2Config(model_path)
config.arch_compat_overrides()
config.max_input_len = 4096
config.max_attention_size = 4096 **2 
tokenizer = ExLlamaV2Tokenizer(config)
model = ExLlamaV2(config)
cache = ExLlamaV2Cache(model, 
                            max_seq_len= config.max_input_len, 
                            lazy=True)

sampler = ExLlamaV2Sampler.Settings()
sampler.temperature = 0.0
sampler.top_p = 1.0
sampler.top_k = 0
sampler.token_repetition_penalty_max = 1.15
sampler.token_repetition_penalty_sustain = 256
sampler.token_repetition_penalty_decay = 128
model.load_autosplit(cache, progress=False)
generator = ExLlamaV2DynamicGenerator(
            model=model,
            cache = cache, 
            tokenizer=tokenizer,
            max_chunk_size = 4096,
            paged = True,
        )
generator.warmup()

print("Model Loaded")

input_path = os.path.join(os.os.environ.get("paper_database_path"), "extracted/run_1/extracted_paper_info_thread.jsonl")
out_path = os.path.join(os.environ.get("paper_database_path"), "extracted/run_1/annotated_papers.jsonl")

t = threading.Thread(target=writer_thread, args=(out_path, annotated_q), daemon=True)
t.start()

list_of_papers = []
with open(input_path, 'r', encoding='utf-8') as f:
    for line in f: 
        row = json.loads(line)
        paper_info = {key: row.get(key) for key in ['doi', 'paper_text']}
        list_of_papers.append(paper_info)

all_papers = [paper['paper_text'] for paper in list_of_papers]
all_paper_ids = [paper['doi'] for paper in list_of_papers]

for paper_id, paper in zip(all_paper_ids , all_papers): 
    limited_paper = chunk_text_by_char_limit(paper,50000)
    need = tokens_needed(tokenizer, limited_paper, max_new_tokens=256)
    print(f"paper{paper_id} needs {need} tokens")
    ensure_capacity(model, tokenizer , need)
    records = batch_call_llm( generator, sampler, tokenizer, paper, paper_id)
    if isinstance(records, dict): 
        annotated_q.put(records)
    
#shutdown
annotated_q.put(SENTINEL)
annotated_q.join()
t.join()
print("Annotation of Papers is Complete")
