
def make_llama3_chat( header_prompt:str , user_text: str, footer_prompt:str) -> str:

    # return system_text
    user_block = (
            f"----- PAPER START -----\n{user_text}\----- PAPER END -----"
        )
        
    return (
        "<|begin_of_text|>"
        "<|start_header_id|>system<|end_header_id|>\n"
        f"{header_prompt}\n"
        "<|eot_id|>"
        "<|start_header_id|>user<|end_header_id|>\n"
        f"{user_block}\n"
        f"{footer_prompt}"
        "<|eot_id|>"
        "<|start_header_id|>assistant<|end_header_id|>\n"
    )





def make_qwen_chat( header_prompt:str , user_text: str, footer_prompt:str) -> str:

    # return system_text
    user_block = (
            f"----- PAPER START -----\n{user_text}\----- PAPER END -----"
        )
    
    
    # Qwen-2.5 / ChatML format
    # Roles are specified after the <|im_start|> token
    return (
        "<|im_start|>system\n"
        f"{header_prompt}<|im_end|>\n"
        "<|im_start|>user\n"
        f"{user_block}\n"
        f"{footer_prompt}<|im_end|>\n"
        "<|im_start|>assistant\n"
    )