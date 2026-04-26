import inspect
from LLM_Agent.util.prompt_functions import make_llama3_chat, make_qwen_chat

def get_model_family_safe(tokenizer):
    """
    Helper to detect 'qwen' or 'llama' by looking at the file path.
    This avoids touching the .arch object which causes crashes.
    """
    if hasattr(tokenizer, "config") and hasattr(tokenizer.config, "model_dir"):
        path_str = str(tokenizer.config.model_dir).lower()
        if "qwen" in path_str: return "qwen"
        if "llama" in path_str: return "llama"
        if "mistral" in path_str: return "llama" 

    if hasattr(tokenizer, "name_or_path"):
        path_str = str(tokenizer.name_or_path).lower()
        if "qwen" in path_str: return "qwen"
        if "llama" in path_str: return "llama"

    class_name = tokenizer.__class__.__name__.lower()
    if "qwen" in class_name: return "qwen"
    
    return "unknown"

def prompt_logic(tokenizer): 
    # Detect family using safe path check
    model_family = get_model_family_safe(tokenizer)

    config = {
        "type": model_family,
        "format_func": None
    }

    if model_family == "qwen":
        config["format_func"] = make_qwen_chat 
    elif model_family == "llama":
        config["format_func"] = make_llama3_chat 
    else:
        # Default to Llama if unknown
        config["format_func"] = make_llama3_chat

    return config
    
def universal_encode(usr_msg, tokenizer):
    sig = inspect.signature(tokenizer.encode)
    params = sig.parameters
    is_exllama = "add_bos" in params
    is_huggingface = "add_special_tokens" in params

    model_family = get_model_family_safe(tokenizer)
    is_qwen = (model_family == "qwen")

    # 3. Apply Logic
    if is_exllama:
        if is_qwen:
            return tokenizer.encode(usr_msg, add_bos=False, add_eos=False)
        else:
            return tokenizer.encode(usr_msg, add_bos=True, add_eos=False)

    elif is_huggingface:
        if is_qwen:
            return tokenizer.encode(usr_msg, add_special_tokens=False)
        else:
            return tokenizer.encode(usr_msg, add_special_tokens=True)
            
    else:
        return tokenizer.encode(usr_msg)
