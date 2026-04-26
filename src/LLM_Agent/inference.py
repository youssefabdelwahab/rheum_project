
import json
import logging
from typing import List, Dict, Any, Optional
from exllamav2.generator import ExLlamaV2DynamicJob



def get_stop_conditions(prompt_format: str, tokenizer):
    """
    Stop when Llama-3/Qwen emits end-of-turn tokens.
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
        
        seen, uniq = set(), []
        for s in stops:
            if s not in seen:
                seen.add(s)
                uniq.append(s)
        return uniq
    elif prompt_format == "llama":
        return [tokenizer.eos_token_id]
    elif prompt_format == "granite":
        return [tokenizer.eos_token_id, "\n\nQuestion:"]
    elif prompt_format == "gemma":
        return [tokenizer.eos_token_id, "<end_of_turn>"]
    else:
        raise ValueError(f"Unknown prompt_format: {prompt_format!r}")
    


def first_json_dict(text: str, required_key: str | None = None):
    """Return the first JSON object embedded in `text`."""
    if not isinstance(text, str):
        return None

    try:
        obj = json.loads(text)
        if isinstance(obj, dict) and (required_key is None or required_key in obj):
            return obj
        if isinstance(obj, str):
            try:
                obj2 = json.loads(obj)
                if isinstance(obj2, dict) and (required_key is None or required_key in obj2):
                    return obj2
            except json.JSONDecodeError:
                pass
    except json.JSONDecodeError:
        pass

    dec = json.JSONDecoder()
    i, n = 0, len(text)
    while i < n:
        j = text.find("{", i)
        if j == -1:
            break
        try:
            obj, end = dec.raw_decode(text, j)
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

def inline_llm_call(generator, tokenizer, sampler, max_new_tokens: int, user_prompt: str, prompt_id: str):
    model_arch = "unknown"
    if hasattr(tokenizer, "config") and hasattr(tokenizer.config, "model_dir"):
        model_arch = str(tokenizer.config.model_dir).lower()
    elif hasattr(tokenizer, "config") and hasattr(tokenizer.config, "arch"):
        model_arch = str(tokenizer.config.arch).lower()

    is_qwen = "qwen" in model_arch
    is_llama = "llama" in model_arch or "mistral" in model_arch

    stop_ids = []
    should_add_bos = True 

    if is_qwen:
        should_add_bos = False
        im_end = tokenizer.single_id("<|im_end|>") 
        if im_end is not None: stop_ids.append(im_end)
    elif is_llama:
        should_add_bos = True
        eot_id = tokenizer.single_id("<|eot_id|>")
        if eot_id is not None: stop_ids.append(eot_id)

    if tokenizer.eos_token_id is not None:
        stop_ids.append(tokenizer.eos_token_id)

    stop_ids = list(set(stop_ids))
    stop_strings = [
        "```", "\nassistant", "\nAssistant", 
        "\nThere ", "\nThe ", "\nThis ", "\nNote:", "\n\nNote",
        "assistant", "assistant\n\n", " Answer:", " Final", " Therefore",
        "}\n", "}\r\n"
    ]

    result = generator.generate(
        prompt=user_prompt,
        gen_settings=sampler, 
        max_new_tokens=max_new_tokens,
        stop_conditions=stop_ids, 
        stop_strings=stop_strings,
        add_bos=should_add_bos, 
        return_tokens=False
    )
   
    out_text = result
    if out_text.startswith(user_prompt):
        completion = out_text[len(user_prompt):].strip()
    else:
        completion = out_text[-len(out_text)+len(user_prompt):].strip() 

    obj = first_json_dict(completion, required_key="trials")
    return {"paper_id": prompt_id, "trials": obj.get("trials", []) if obj else []}