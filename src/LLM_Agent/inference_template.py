
from exllamav2 import ExLlamaV2, ExLlamaV2Tokenizer , ExLlamaV2Config , ExLlamaV2Cache 
from exllamav2.generator import ExLlamaV2DynamicGenerator , ExLlamaV2Sampler , ExLlamaV2DynamicJob
from exllamav2.architecture import ExLlamaV2ArchParams
from typing import Sequence, Optional, List, Dict, Any
import logging, os, sys, atexit, signal, json
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

env_path = "/work/robust_ai_lab/shared/env_vars/rheum_project/env_vars.sh" # export SCRIPT_ENV_FILE=/full/path/to/env_vars.sh
if not env_path:
    raise RuntimeError("SCRIPT_ENV_FILE is not set")

env_path = str(Path(env_path).expanduser())
ok = load_dotenv(dotenv_path=env_path, override=False)
if not ok:
    raise FileNotFoundError(f"Could not load env file at {env_path}")

dir_path = os.getenv("LOG_DIR")

log_dir = os.path.join(dir_path, "paper_annotation")
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir,  f"annotation_{datetime.now():%Y-%m-%d}.log")

def cleanup():
    try:
        logging.info("Shutting down (atexit cleanup)…")
    except Exception:
        pass
    # Ensure all logging handlers flush/close — prevents lingering .nfs* files
    try:
        logging.shutdown()
    except Exception:
        pass



logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename=str(log_file),
    filemode="a",
    encoding="utf-8",
)

atexit.register(cleanup)

LLAMA3_TEMPLATE = (
    "<|begin_of_text|>"
    "<|start_header_id|>system<|end_header_id|>\n\n{system}<|eot_id|>"
    "<|start_header_id|>user<|end_header_id|>\n\n{user}<|eot_id|>"
    "<|start_header_id|>assistant<|end_header_id|>\n\n"
)
#  "Return JSON with fields: trials: [{name, clinical registration number}].\n\n"
#  "----- PAPER START -----\n"
#         f"{cleaned}\n"
#         "----- PAPER END -----\n"
#         )


def make_llama3_chat( user_text: str) -> str:
    system_text = (
        """
            Task: Extract every clinical trial mentioned in the paper.

            For each trial capture:
            - "name": the trial name as written, or null if no name appears.
            - "registration_number": the exact identifier as written (NCT, ISRCTN, EudraCT, CTRI, ChiCTR, ANZCTR, UMIN-CTR, jRCT), or null if none appears.

            Rules:
            1) If only a registration number is present → name=null.
            2) If only a name is present → registration_number=null.
            3) Deduplicate by registration_number; if null, deduplicate by case-insensitive name.
            4) Use only information present in the paper.
            5) Dont include any extra text or markdown formatting
         

            Output ONLY strict JSON:
            {"trials":[{"name":<string|null>,"registration_number":<string|null>}]}
            If none, output {"trials":[]}.
        """
    )
    # return system_text
    user_block = (
            f"----- PAPER START -----\n{user_text}\----- PAPER END -----"
        )
        
    return (
        "<|begin_of_text|>"
        "<|start_header_id|>system<|end_header_id|>\n"
        f"{system_text}\n"
        "<|eot_id|>"
        "<|start_header_id|>user<|end_header_id|>\n"
        f"{user_block}\n"
        "<|eot_id|>"
        "<|start_header_id|>assistant<|end_header_id|>\n"
    )


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


def handle_signal(signum, frame):
    try:
        logging.warning(f"Received signal {signum}; running cleanup.")
    except Exception:
        pass
    cleanup()
    # Exit with a code that reflects the signal (optional)
    sys.exit(128 + signum)

signal.signal(signal.SIGTERM, handle_signal)
signal.signal(signal.SIGINT,  handle_signal)

def load_model_on_gpu( 
        model_path:str, 
        max_ctx:int, 
        max_tokens:int,
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

        # assert isinstance(max_ctx, int) and max_ctx % 256 == 0, \
        #     f"max_ctx must be multiple of 256; got {max_ctx}"
        # assert isinstance(max_input_len, int), "max_input_len must be int"
        # max_input_len = min(max_input_len, max_ctx)
        assert max_chunk_size > 0 and isinstance(max_chunk_size, int)
        # torch.cuda.set_device(gpu_id)
        model_path = model_path
        config = ExLlamaV2Config(model_path)
        config.arch_compat_overrides()
        config.max_input_len = config.max_input_len
        config.max_attention_size = max_chunk_size * max_chunk_size
        tokenizer = ExLlamaV2Tokenizer(config)
        


        model = ExLlamaV2(config)
        cache = ExLlamaV2Cache(model, 
                            max_seq_len= config.max_input_len, 
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
 

# def chunk_text_by_char_limit(text:str, limit:int):
#     for i in range(0, len(text), limit):
#         return text[i:i + limit] 

def truncate_each_to_limit_wordwise(items, limit: int):
    
    if limit < 0:
        raise ValueError("limit must be >= 0")

    out = []
    for s in items:
        # normalize to string, remove zero-width spaces, trim
        t = "" if s is None else str(s)
        t = t.replace("\u200b", "").strip()

        if len(t) <= limit:
            out.append(t)
            continue

        cut = t[:limit]
        pos = cut.rfind(" ")
        # if we found whitespace before the limit, cut there; otherwise hard cut
        out.append((cut[:pos].rstrip()) if pos != -1 else cut)
    return out

# def chunk_text_by_char_limit(text: str, limit: int) -> list[str]:
#     return [text[i:i+limit] for i in range(0, len(text), limit)]






def batch_call_llm(generator, 
                   sampler, 
                   tokenizer, 
                   user_prompts: list, 
                   ids: list) -> str:
    prompt_format = 'llama3'

   

    if ids is None:
        ids = [str(i) for i in range(len(user_prompts))]
    assert len(ids) == len(user_prompts), "Length of ids must match length of prompts"

    jobs = []
    jobs_to_idx = {}
    # job_prompt_text: Dict[Any, str] = {}

    for i, (pid, ptxt) in enumerate(zip(ids, user_prompts)): 
        usr_msg = make_llama3_chat(ptxt)
       
        input_ids = tokenizer.encode(usr_msg, encode_special_tokens = True)
       
        job = ExLlamaV2DynamicJob( 
            input_ids=input_ids,
            max_new_tokens = 128, 
            stop_conditions = get_stop_conditions(prompt_format, tokenizer), 
            # stop_conditions = [tokenizer.single_id("}")] or [],
            add_bos = False,
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
            

                rec = {
                "doi": ids[i],
                "full_completion": r.get('full_completion'),
                "new_tokens": r.get("new_tokens", 0),
                "prompt_tokens": r.get("prompt_tokens", 0),
                "cached_tokens": r.get("cached_tokens", 0),
                "time_enqueued_s": r.get("time_enqueued", 0.0),
                "time_prefill_s": r.get("time_prefill", 0.0),
                "time_generate_s": r.get("time_generate", 0.0),
                "eos_reason": r.get("eos_reason", "eos"),
            }
                records[i] = rec
                logging.info(f"Completed job for id {ids[i]}")
    logging.info("All jobs completed")
    return [rec for rec in records if rec is not None]


def first_json_dict(text: str, required_key: str | None = None):
    """
    Return the first JSON object embedded in `text`.
    - Works if `text` is a JSON object, a JSON *string* containing an object, or
      free text with one/more {...} blocks.
    - If `required_key` is set, only return an object that contains that key.
    Returns None if nothing suitable is found.
    """
    if not isinstance(text, str):
        return None

    # 1) Try parsing the whole thing directly
    try:
        obj = json.loads(text)
        # object already
        if isinstance(obj, dict) and (required_key is None or required_key in obj):
            return obj
        # it's a JSON *string* that itself holds JSON -> decode again
        if isinstance(obj, str):
            try:
                obj2 = json.loads(obj)
                if isinstance(obj2, dict) and (required_key is None or required_key in obj2):
                    return obj2
            except json.JSONDecodeError:
                pass
    except json.JSONDecodeError:
        pass

    # 2) Scan for the first {...} and raw-decode from there
    dec = json.JSONDecoder()
    i, n = 0, len(text)
    while i < n:
        j = text.find("{", i)
        if j == -1:
            break
        try:
            obj, end = dec.raw_decode(text, j)
            # if the parsed thing is itself a JSON string, try decoding it too
            if isinstance(obj, str):
                try:
                    obj2 = json.loads(obj)
                    if isinstance(obj2, dict) and (required_key is None or required_key in obj2):
                        return obj2
                except json.JSONDecodeError:
                    pass
            if isinstance(obj, dict) and (required_key is None or required_key in obj):
                return obj
            i = end
        except json.JSONDecodeError:
            i = j + 1

    return None

def inline_llm_call(generator,  tokenizer , sampler, user_prompt, prompt_id): 

    prompt = make_llama3_chat(user_prompt)
    prompt_encoded = tokenizer.encode(prompt, add_bos=True, encode_special_tokens=True)

    stop_strings = [
        "```",                 # any fence
        "\nassistant",         # chat template bleed
        "\nAssistant",         # capitalized variant
        "\nThere ", "\nThe ", "\nThis ", "\nNote:", 
        "\n\nNote",  # typical commentary starts
        "assistant",           # bare token, just in case
        "assistant\n\n",
        " Answer:", " Final", " Therefore",
        "}\n", "}\r\n",        # most models add a newline after JSON
    ]


    result = generator.generate(
        prompt = prompt,
        gen_settings = sampler, 
        max_new_tokens = 128,
        stop_conditions = [tokenizer.eos_token_id] ,
        stop_strings = stop_strings,
        add_bos = True,
        return_tokens  = False
        )
   
    if isinstance(result, tuple):
            out_text, out_tokens = result
            # Slice off the prompt tokens; decode only the completion
            new_tokens = out_tokens[len(prompt_encoded):]
            completion = tokenizer.decode(new_tokens).strip()
    else:
    #         # Fallback: result is just text (shouldn't happen with return_tokens=True, but safe)
        out_text = result
        #     # Remove the prompt prefix carefully (handles exact string match)
        completion = out_text[len(prompt):].strip() if out_text.startswith(prompt) else out_text.strip()
    obj = first_json_dict(completion ,required_key="trials")
    logging.info("Annotated Paper")

    return {"doi": prompt_id, "trials": obj.get("trials", []) if obj else []}