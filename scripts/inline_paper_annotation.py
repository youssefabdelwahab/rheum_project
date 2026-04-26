import os
import sys
import json
import logging
import argparse
import atexit
import signal
import threading
import queue
import torch
import socket
from pathlib import Path
from datetime import datetime
from glob import glob
from typing import List, Dict

from exllamav2 import ExLlamaV2, ExLlamaV2Tokenizer, ExLlamaV2Config, ExLlamaV2Cache 
from exllamav2.generator import ExLlamaV2DynamicGenerator, ExLlamaV2Sampler 

from LLM_Agent.inference import inline_llm_call 
from LLM_Agent.util.tokenizer_args import universal_encode, prompt_logic


resize_lock = threading.RLock()
annotated_q = queue.Queue(maxsize=50)
SENTINEL = object()

def parse_args():
    ap = argparse.ArgumentParser(description="Extract clinical trials from Markdown papers using ExLlamaV2.")
    ap.add_argument("--model-path", type=Path, required=True, help="Local path to the ExLlamaV2 formatted model.")
    ap.add_argument("--input-dir", type=Path, required=True, help="Directory containing .md paper files.")
    ap.add_argument("--out-dir", type=Path, required=True, help="Directory to save the resulting .jsonl annotations.")
    ap.add_argument("--log-dir", type=Path, default=Path("./logs"), help="Directory for runtime logs.")
    ap.add_argument("--max-ctx", type=int, default=122880, help="Maximum context window size (Default: 122880).")
    ap.add_argument("--max-new", type=int, default=1024, help="Maximum new tokens to generate (Default: 1024).")
    ap.add_argument("--chunk-size", type=int, default=8192, help="Chunk size for attention processing (Default: 8192).")
    return ap.parse_args()

def setup_logging(log_dir: Path):
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"annotation_{datetime.now():%Y-%m-%d}.log"
    
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        filename=str(log_file),
        filemode="a",
        encoding="utf-8",
    )
    return log_file

def cleanup():
    try:
        logging.info("Shutting down (atexit cleanup)…")
    except Exception:
        pass
    try:
        logging.shutdown()
    except Exception:
        pass

def handle_signal(signum, frame):
    try:
        logging.warning(f"Received signal {signum}; running cleanup.")
    except Exception:
        pass
    cleanup()
    sys.exit(128 + signum)


def writer_thread(path: Path, q: queue.Queue):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, 'a', encoding='utf-8') as f:
        while True:
            item = q.get()
            try: 
                if item is SENTINEL: 
                    break
                if item is None: 
                    continue
                
                batch = item if isinstance(item, (list, tuple)) else [item]   
                for rec in batch: 
                    if not rec or not isinstance(rec, dict): 
                        continue

                    cleaned_rec = {
                        "paper_id": rec.get("paper_id"), 
                        "trials": rec.get("trials", []), 
                        "truncated": rec.get("truncated", False), 
                        "error": rec.get('error', None)
                    }
                    f.write(json.dumps(cleaned_rec, ensure_ascii=False) + "\n")
                    f.flush()
            finally: 
                q.task_done()
def ensure_context_length(paper_content: str, tokenizer, prompt_logic_func, max_prompt_tokens: int) -> tuple[str, bool]:
    header_prompt = """
Task: You are a Clinical Research Auditor. Your goal is to extract all PRIMARY clinical trials or patient studies described in this paper using a Chain of Verification process.

/// SEARCH STRATEGY ///
Prioritize your search in this order:
1. **Abstract**: Look for the study design (e.g., "We conducted a randomized trial") and Registration IDs (NCT, ISRCTN).
2. **Methods**: Look for patient recruitment details, intervention descriptions, and ethics approval numbers.
3. **Results**: Confirm the study actually took place and generated data.
4. **Introduction/Discussion**: BE CAREFUL. These sections contain CITATIONS to *other* papers. Do not extract trials referenced here unless they are the subject of the current analysis.

/// DEFINITION OF A TRIAL ///
Include:
- Explicitly named clinical trials (e.g., "The ASCOT Trial").
- Unnamed but structured patient interventions (e.g., "A comparison of drug X vs Y in 50 patients").
- Historical/unregistered studies common in older rheumatology papers.
- Trials mentioned in the Abstract/Methods even without a formal ID.

Exclude:
- Animal studies (mice, rats, in vitro).
- Pure literature reviews (unless it's a meta-analysis of specific trials).
- Trials that are merely CITED as background work.

/// VERIFICATION STEPS ///
Before generating the JSON, you must perform a verification check:
1. Is the candidate a *citation* or the *current study*? (Discard citations).
2. Is the NCT ID explicitly in the text, or are you guessing? (Do not guess).
3. Does the text describe an intervention on humans? (Discard animal/cell studies).
"""

    footer_prompt = """
/// OUTPUT FORMAT ///

Step 1: Output a [VERIFICATION] block where you briefly list candidates and rule out false positives.
Step 2: Output the final JSON object.

Example Structure:

[VERIFICATION]
- Candidate "RECOVERY Trial": Found in Introduction. Context says "Previous studies like...". VERDICT: Exclude (Citation).
- Candidate "NCT01234567": Found in Abstract. Context says "We registered this study as...". VERDICT: Keep.
[END VERIFICATION]

{
  "trials": [
    {
      "name": "Exact Trial Name OR Descriptive Name",
      "registration_number": "NCT00000000 or null"
    }
  ]
}

If no valid trials are found after verification, output: {"trials": []}
Do NOT use markdown code fences (```json). Just output the raw text and JSON.
"""
    
    paper_tokens = universal_encode(paper_content, tokenizer)
    prompt_logic_dict = prompt_logic_func(tokenizer)
    
    if hasattr(paper_tokens, "shape"):
        context_len = paper_tokens.shape[-1]
    else:
        context_len = len(paper_tokens)
        
    is_truncated = False

    if context_len > max_prompt_tokens:
        is_truncated = True
        ratio = max_prompt_tokens / context_len
        target_chars = int(len(paper_content) * ratio * 0.95)
        paper_content = paper_content[:target_chars]
        paper_content = paper_content[:paper_content.rfind(" ")] + "... [TRUNCATED]"

    formatted_prompt = prompt_logic_dict['format_func'](header_prompt, paper_content, footer_prompt)
    return formatted_prompt, is_truncated

def load_md_papers(input_path: Path) -> List[Dict[str, str]]:
    list_of_papers = []
    for filename in os.listdir(input_path):
        if not filename.endswith(".md"): 
            continue
        paper_path = input_path / filename
        with paper_path.open("r", encoding="utf-8") as f:
            list_of_papers.append({"paper_id": paper_path.stem, "paper_text": f.read()})
    return list_of_papers

def main():
    args = parse_args()
    
    # Initialization
    log_file = setup_logging(args.log_dir)
    atexit.register(cleanup)
    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    print(f"Log file initialized at: {log_file}")
    print(f"HOST: {socket.gethostname()} | CUDA: {torch.cuda.is_available()} | GPUs: {torch.cuda.device_count()}")
    
    safe_headroom = 500 
    max_prompt_tokens = args.max_ctx - args.max_new - safe_headroom

    # Load Model Configuration
    config = ExLlamaV2Config(str(args.model_path))
    config.arch_compat_overrides()
    config.max_input_len = args.max_ctx
    config.max_attention_size = max(getattr(config, "max_attention_size", 0), args.chunk_size * args.chunk_size)

    tokenizer = ExLlamaV2Tokenizer(config)
    model = ExLlamaV2(config)
    model.load()

    big_cache = ExLlamaV2Cache(model, max_seq_len=args.max_ctx, lazy=False)

    sampler = ExLlamaV2Sampler.Settings()
    sampler.temperature = 0.0 
    sampler.top_p = 1.0
    sampler.token_healing = False 
    sampler.stop_on_eos = True

    generator = ExLlamaV2DynamicGenerator(
        model=model,
        cache=big_cache,
        tokenizer=tokenizer,
        sampler=sampler,
        max_chunk_size=args.chunk_size,
        paged=True,
    )
    print("Model Loaded Successfully.")

    # Prepare I/O
    args.out_dir.mkdir(parents=True, exist_ok=True)
    out_file = args.out_dir / f"annotated_papers_{datetime.now():%Y-%m-%d_%H-%M}.jsonl"

    t = threading.Thread(target=writer_thread, args=(out_file, annotated_q), daemon=True)
    t.start()

    # Process Papers
    papers = load_md_papers(args.input_dir)
    print(f"Loaded {len(papers)} papers from {args.input_dir}")

    for paper in papers:
        paper_id = paper['paper_id']
        paper_text = paper['paper_text']
        
        encoded_paper = universal_encode(paper_text, tokenizer)
        paper_token_len = encoded_paper.shape[-1] if hasattr(encoded_paper, "shape") else len(encoded_paper)

        if paper_token_len >= 135000:
            print(f'Paper {paper_id} Too Big ({paper_token_len}), Risk of losing context')
            annotated_q.put({
                'paper_id': paper_id, 
                'trials': [], 
                'truncated': True, 
                'error': 'Paper Too Big (>135k), annotate manually'
            })
            continue
        
        paper_with_prompt, truncated_flag = ensure_context_length(paper_text, tokenizer, prompt_logic, max_prompt_tokens)
        
        print(f"Annotating {paper_id}...")
        try:
            records = inline_llm_call(generator, tokenizer, sampler, args.max_new, paper_with_prompt, paper_id)
            records['truncated'] = truncated_flag
            print(f"Successfully Annotated {paper_id}")
            annotated_q.put(records)
        except Exception as e:
            print(f"Error annotating {paper_id}: {str(e)}")
            annotated_q.put({'paper_id': paper_id, 'trials': [], 'truncated': truncated_flag, 'error': str(e)})

    # Shutdown
    annotated_q.put(SENTINEL)
    t.join()
    print(f"Annotation complete. Output saved to {out_file}")

if __name__ == "__main__":
    main()


