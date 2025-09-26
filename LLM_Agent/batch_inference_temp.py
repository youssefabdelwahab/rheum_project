
import asyncio
from exllamav2 import ExLlamaV2, ExLlamaV2Tokenizer , ExLlamaV2Config , ExLlamaV2Cache 
from exllamav2.generator import ExLlamaV2DynamicGenerator , ExLlamaV2Sampler , ExLlamaV2DynamicJob
from exllamav2.architecture import ExLlamaV2ArchParams
from typing import Sequence, Optional, List, Dict, Any
import logging, os
from pathlib import Path
from datetime import datetime



import torch


# os.environ["CUDA_HOME"] = "/global/software/cuda/12.4.1"
# os.environ["TORCH_EXTENSIONS_DIR"] = "/work/robust_ai_lab/shared/.cache/torch_ext"  # your shared cache
# os.environ["TORCH_CUDA_ARCH_LIST"] = "9.0;8.0"



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

LLAMA3_TEMPLATE = (
    "<|begin_of_text|>"
    "<|start_header_id|>system<|end_header_id|>\n\n{system}<|eot_id|>"
    "<|start_header_id|>user<|end_header_id|>\n\n{user}<|eot_id|>"
    "<|start_header_id|>assistant<|end_header_id|>\n\n"
)

def make_llama3_user_msg(paper_text: str) -> str:
    cleaned = paper_text.replace("\u200b", "").strip()
    return (
        "You are given the full text of a medical research paper.\n\n"
        "TASK: Extract the main clinical trial name(s) and their registration number(s)\n"
        "(e.g., NCT numbers, ISRCTN, EudraCT).\n\n"
        "Return JSON with fields: trials: [{name, registry_id}].\n\n"
        "----- PAPER START -----\n"
        f"{cleaned}\n"
        "----- PAPER END -----\n\n"
        "Now answer using only information present in the paper."
    )

def format_prompt(prompt_format, sp, p):
    """
    Returns a chat-formatted prompt string. For Llama-3, we emit the
    system -> user -> assistant headers that the tokenizer expects.
    """
    if prompt_format == "llama3":
        sys_txt = (sp or "You are a helpful assistant.").strip()
        usr_txt = (p or "").strip()
        return LLAMA3_TEMPLATE.format(system=sys_txt, user=usr_txt)

    elif prompt_format == "llama":
        return f"<s>[INST] <<SYS>>\n{sp}\n<</SYS>>\n\n{p} [/INST]"

    elif prompt_format == "granite":
        return f"System:\n{sp}\n\nQuestion:\n{p}\n\nAnswer:\n"

    elif prompt_format == "chatml":
        return (
            f"<|im_start|>system\n{sp}<|im_end|>\n"
            f"<|im_start|>user\n{p}<|im_end|>\n"
            f"<|im_start|>assistant\n"
        )

    elif prompt_format == "gemma":
        return f"<bos><start_of_turn>user\n{p}<end_of_turn>\n<start_of_turn>model\n"

    else:
        raise ValueError(f"Unknown prompt_format: {prompt_format!r}")


def get_stop_conditions(prompt_format, tokenizer):
    """
    Stop when Llama-3 emits end-of-turn <|eot_id|>. Also include eos_token_id
    if the tokenizer defines one (harmless to have both).
    """
    if prompt_format == "llama3":
        stops = []
        try:
            eot_id = tokenizer.single_id("<|eot_id|>")
            if eot_id is not None and eot_id >= 0:
                stops.append(eot_id)
        except Exception:
            pass
        eos = getattr(tokenizer, "eos_token_id", None)
        if eos is not None and eos >= 0:
            stops.append(eos)
        # dedup while preserving order
        seen, uniq = set(), []
        for s in stops:
            if s not in seen:
                seen.add(s); uniq.append(s)
        return uniq

    elif prompt_format == "llama":
        return [tokenizer.eos_token_id]

    elif prompt_format == "granite":
        return [tokenizer.eos_token_id, "\n\nQuestion:"]

    elif prompt_format == "gemma":
        return [tokenizer.eos_token_id, "<end_of_turn>"]

    else:
        raise ValueError(f"Unknown prompt_format: {prompt_format!r}")


def load_model_on_gpu( 
        model_path:str, 
        max_ctx:int, 
        max_chunk_size:int, 
        max_input_len:int, 
        temp:float, 
        top_p:float, 
        top_k:int,
        tok_rep_max_pen:float, 
        tok_rep_stn_pen:int, 
        tok_rep_pen_decay:int
        ):

    try:
    # Set device before model loading

        assert isinstance(max_ctx, int) and max_ctx % 256 == 0, \
            f"max_ctx must be multiple of 256; got {max_ctx}"
        assert isinstance(max_input_len, int), "max_input_len must be int"
        max_input_len = min(max_input_len, max_ctx)
        assert max_chunk_size > 0 and isinstance(max_chunk_size, int)

        # torch.cuda.set_device(gpu_id)
        model_path = model_path
        # model_path = "/work/robust_ai_lab/exl2_models/llama_nets/3_bpw/Llama-3.1-70B-Instruct-exl2"
        config = ExLlamaV2Config(model_path)
        config.arch_compat_overrides()
        config.max_input_len = max_input_len
        effective_chunk = min(max_chunk_size, max_input_len)
        config.max_attention_size = effective_chunk * effective_chunk
        tokenizer = ExLlamaV2Tokenizer(config)



        model = ExLlamaV2(config)
        cache = ExLlamaV2Cache(model, 
                            max_seq_len=max_ctx, 
                            lazy=True)
        sampler = ExLlamaV2Sampler.Settings()
        sampler.temperature = temp
        sampler.top_p = top_p
        sampler.top_k = top_k
        sampler.token_repetition_penalty_max = tok_rep_max_pen
        sampler.token_repetition_penalty_sustain = tok_rep_stn_pen
        sampler.token_repetition_penalty_decay = tok_rep_pen_decay

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
        print("Model loaded on GPU")

        return generator, sampler, tokenizer
    except Exception as e:
        logging.error(f"Error loading model on GPU : {e}")
        print(f"Error loading model on GPU : {e}")
        raise e
 





def batch_call_llm(generator, sampler, tokenizer, system_prompt: str , user_prompts: str, ids: Optional[Sequence[str]] = None) -> str:
    prompt_format = 'llama3'
    # ban_strings = None
    # filters = None
    if ids is None:
        ids = [str(i) for i in range(len(user_prompts))]
    assert len(ids) == len(user_prompts), "Length of ids must match length of prompts"

    jobs = []
    jobs_to_idx = {}

    for i, (pid, ptxt) in enumerate(zip(ids, user_prompts)): 
        usr_msg = make_llama3_user_msg(ptxt)
        fprompt = format_prompt(
            prompt_format, 
            system_prompt, 
            usr_msg
        )
        input_ids = tokenizer.encode(fprompt, encode_special_tokens = True)
        job = ExLlamaV2DynamicJob( 
            input_ids=input_ids,
            max_new_tokens = 256, 
            stop_conditions = get_stop_conditions(prompt_format, tokenizer), 
            # banned_strings = ban_strings, 
            # filters = filters,
            gen_settings = sampler,

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
                i = jobs_to_idx[job]

                # text = r.get("text") or r.get('full_text')

                # if text is None:
                #     ids = r.get("full_completion", [])
                #     if hasattr(ids, "tolist"):
                #         ids = ids.tolist()
                #     text = tokenizer.decode(ids, decode_special_tokens=True)[0]

            #     in_prompt = tokenizer.decode(
            #     job.sequences[0].input_ids.torch(),
            #     decode_special_tokens=True
            # )[0]
                
                rec = {
                "doi": ids[i],
                "full_completion": r.get("full_completion"),
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