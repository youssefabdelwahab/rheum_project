
import asyncio
from exllamav2 import ExLlamaV2, ExLlamaV2Tokenizer , ExLlamaV2Config , ExLlamaV2Cache 
from exllamav2.generator import ExLlamaV2DynamicGenerator , ExLlamaV2Sampler , ExLlamaV2DynamicJob
from exllamav2.architecture import ExLlamaV2ArchParams
from exllamav2.utils import ExLlamaV2Utils
from typing import Sequence, Optional, List, Dict, Any
import logging, os
from pathlib import Path
from datetime import datetime



import torch



# max_ctx = 100_000
# max_chunk_size = 25_000
# paged = True
# prompt_format = 'llama'
# system_prompt = ""
# ban_strings = None
# filters = None
# healing = True

log_dir = Path("logs/annotation_inference")
log_dir.mkdir(parents=True, exist_ok=True)
log_file = log_dir / f"app_{datetime.now():%Y-%m-%d}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename=str(log_file),
    filemode="a",
    encoding="utf-8",
)


def load_model_on_gpu( model_path:str):

    try:
    # Set device before model loading



        max_ctx = 20_000
        max_chunk_size = 4096
        # torch.cuda.set_device(gpu_id)
        model_path = model_path
        # model_path = "/work/robust_ai_lab/exl2_models/llama_nets/3_bpw/Llama-3.1-70B-Instruct-exl2"
        config = ExLlamaV2Config(model_path)
        config.arch_compat_overrides()
        config.max_input_len = 4096
        config.max_attention_size = max_chunk_size ** 2
        tokenizer = ExLlamaV2Tokenizer(config)



        model = ExLlamaV2(config)
        cache = ExLlamaV2Cache(model, 
                            max_seq_len=max_ctx, 
                            lazy=True)
        sampler = ExLlamaV2Sampler.Settings()
        sampler.temperature = 0.2
        sampler.top_p = 0.9
        sampler.top_k = 40

        model.load_autosplit(cache, progress=False)

        generator = ExLlamaV2DynamicGenerator(
            model=model,
            cache = cache, 
            tokenizer=tokenizer,
            max_chunk_size = max_chunk_size,
            paged = True,
        )
        generator.warmup()
        logging.info("Model loaded on GPU")
        return generator, sampler, tokenizer
    except Exception as e:
        logging.error(f"Error loading model on GPU : {e}")
        raise e
 





def batch_call_llm(generator, sampler , tokenizer, system_prompt: str , user_prompts: str, ids: Optional[Sequence[str]] = None) -> str:
    
    prompt_format = 'llama'
    ban_strings = None
    filters = None
    healing = True
    if ids is None:
        ids = [str(i) for i in range(len(user_prompts))]
    assert len(ids) == len(user_prompts), "Length of ids must match length of prompts"

    jobs = []
    jobs_to_idx = {}

    for i, (pid, ptxt) in enumerate(zip(ids, user_prompts)): 
        fprompt = ExLlamaV2Utils.format_prompt(
            prompt_format, 
            system_prompt, 
            ptxt
        )
        input_ids = tokenizer.encode(fprompt, encode_special_tokens = True)
        job = ExLlamaV2DynamicJob( 
            input_ids=input_ids,
            max_new_tokens = 512, 
            stop_conditions = ExLlamaV2Utils.stop_conditions_from_format(prompt_format, tokenizer), 
            banned_strings = ban_strings, 
            filters = filters,
            token_healing = healing, 
            gen_settings = sampler

        )
        jobs.append(job)
        jobs_to_idx[job] = i
    generator.enqueue(jobs)
    records: List[Optional[Dict[str, Any]]] = [None] * len(user_prompts)


    while generator.num_remaining_jobs(): 
        results = generator.iterate()
        for r in results: 
            if r['stage'] == 'streaming' and r["eos"]: 
                job = r['job']
            #     in_prompt = tokenizer.decode(
            #     job.sequences[0].input_ids.torch(),
            #     decode_special_tokens=True
            # )[0]
                i = jobs_to_idx[job]
                rec = {
                "doi": ids[i],
                "full_completion": r.get("full_completion", ""),
                "new_tokens": r.get("new_tokens", 0),
                "prompt_tokens": r.get("prompt_tokens", 0),
                "cached_tokens": r.get("cached_tokens", 0),
                "time_enqueued_s": r.get("time_enqueued", 0.0),
                "time_prefill_s": r.get("time_prefill", 0.0),
                "time_generate_s": r.get("time_generate", 0.0),
                "eos_reason": r.get("eos_reason", "eos"),
            }
                records[i] = rec
                logging.info(f"Completed job for id {ids[i]}: {rec}")
    logging.info("All jobs completed")
    return [rec for rec in records if rec is not None]

        # with open(os.path.join(output_path, 'completions.jsonl'), 'w', encoding='utf-8') as f: 
        #     for rec in finished: 
        #         f.write(json.dumps(rec, ensure_ascii=False) + '\n')