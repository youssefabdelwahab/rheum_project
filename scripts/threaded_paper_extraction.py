"""
paper_extraction_threads.py
===========================

High-level asyncio pipeline that:

1. Reads a CSV of article metadata (paper_id, title, DOI, cross-ref link).
2. Resolves a **direct PDF URL** using :py:func:`functions.extraction_functions.extract_text_with_pdf_resolver`.
3. Extracts text content from the PDF.
4. Streams structured results to two `.jsonl` files (extracted / unextracted)
   using a shared queue and writer consumer pattern with asyncio.

Each row is processed concurrently by multiple async workers pulling tasks
from a shared queue. Extracted and unextracted records are pushed into
two dedicated queues and written by a single consumer each.

Run it from the project root or sub-dir; paths resolve relative to cwd.

Usage
-----
```bash
python paper_extraction_threads.py
"""

import os, sys,asyncio,json
import aiofiles , aiocsv
cwd = os.getcwd()
parent_folder = os.path.abspath(os.path.join(cwd, ".."))
sys.path.append(parent_folder)
from typing import Optional
from pathlib import Path
from functions.extraction_functions import extract_text_with_pdf_resolver
from dotenv import load_dotenv
from datetime import datetime


env_path = "/work/robust_ai_lab/shared/env_vars/rheum_project/env_vars.sh"  # export SCRIPT_ENV_FILE=/full/path/to/env_vars.sh
if not env_path:
    raise RuntimeError("SCRIPT_ENV_FILE is not set")

env_path = str(Path(env_path).expanduser())
ok = load_dotenv(dotenv_path=env_path, override=False)
if not ok:
    raise FileNotFoundError(f"Could not load env file at {env_path}")
print("Loaded Env File")

repo_root = os.getenv("ROOT_DIR")
paper_info_file = os.getenv("PAPER_INFO_FILE")
shared_folder = os.path.join(repo_root, "shared")
print("Loaded Environment Variables Successfully")
date_str = datetime.now().strftime("%Y-%m-%d") 



extract_save_folder = os.path.join(shared_folder, "research_paper_database/pdf_paper_extracts", f"extracted_papers_{date_str}")
unextracted_save_folder = os.path.join(shared_folder, "research_paper_database/pdf_paper_extracts" f"unextracted_papers_{date_str}")
pdf_file_dir = os.path.join(extract_save_folder, "pdfs")


for folder in (extract_save_folder, unextracted_save_folder):
    os.makedirs(folder, exist_ok=True)

print("Set Up Folders for Storage Successfully")
    
extract_meta_path = os.path.join(extract_save_folder, "extracted_paper_meta_thread.jsonl")
unextracted_meta_path = os.path.join(unextracted_save_folder, "unextracted_paper_meta_thread.jsonl")


async def load_papers_from_csv(task_queue: asyncio.Queue):
    """
    Asynchronously reads a CSV file containing research paper metadata
    and enqueues each row as a dictionary into a shared asyncio task queue.

    Parameters
    ----------
    task_queue : asyncio.Queue
        A shared task queue where each item is a dictionary containing
        information needed to resolve and extract the paper's PDF.

    Notes
    -----
    - The CSV is expected to have columns: [paper_id, title, doi, crossref_link]
    - Each row is transformed into a dictionary with placeholders for
      `pdf_url`, `paper_text`, and `resolver_error`, which will be filled in
      during extraction.
    - This function acts as the "loader" or "input feeder" for the async pipeline.
    """

    async with aiofiles.open(paper_info_file, 'r') as f:
        reader = aiocsv.AsyncReader(f)

        # Skip the header row
        header = await reader.__anext__()

        seen_ids = set()


        # Read each data row and add it to the task queue
        async for row in reader:
            paper_id = row[0]
            paper_title = row[1]
            paper_doi = row[2]
            paper_link = str(row[3])  # ensure it's a string

            if paper_id in seen_ids:
            # optional: log it
                print(f"[loader] skipping duplicate paper_id {paper_id}")
                continue
            seen_ids.add(paper_id)

            # Create a task dictionary to be processed later
            task_queue.put_nowait({
                "doi": paper_doi,
                "title": paper_title,
                "crossref_paper_link": paper_link,
                "paper_id": paper_id,
                "pdf_url": "",           # will be filled by resolver
                "pdf_bytes": "",        # will be filled after extraction
                "resolver_error": ""     # will be filled if an error occurs
            })



async def extracting_pdf(task_queue: asyncio.Queue,
                         extracted_queue: asyncio.Queue,
                         unextracted_queue: asyncio.Queue,
                         worker_id: int) -> None:
    """
    Async worker function that consumes tasks from a shared queue, resolves
    the PDF for each paper, extracts the text, and enqueues the result into
    either the extracted or unextracted queue.

    Parameters
    ----------
    task_queue : asyncio.Queue
        Queue containing dictionaries of paper metadata to process.

    extracted_queue : asyncio.Queue
        Queue to collect successfully extracted paper records.

    unextracted_queue : asyncio.Queue
        Queue to collect paper records that failed to resolve or extract.

    worker_id : int
        Numeric identifier for the worker (used for logging/debugging).

    Behavior
    --------
    - Runs in a loop until it receives a `None` item (a "poison pill") to shut down.
    - Uses `extract_text_with_pdf_resolver()` to resolve and extract PDF content.
    - Catches and handles timeout errors, missing DOI errors, and general exceptions.
    - Places completed records into the appropriate queue based on success/failure.
    """

    while True:
        paper_dict = await task_queue.get()


        try:
            if paper_dict is None:
                print(f"Worker {worker_id} finished processing.")
                break

            paper_doi = paper_dict.get("doi")
            paper_id = paper_dict.get("paper_id")
            paper_url = paper_dict.get("crossref_paper_link")

            try:
                result = await extract_text_with_pdf_resolver(
                    doi=paper_doi,
                    paper_id=paper_id,
                    cross_ref_paper_link=paper_url,
                    selector_timeout=40000,
                )

                # 1) Nothing came back
                if result is None:
                    paper_dict["resolver_error"] = "Unknown Resolver Error"
                    await unextracted_queue.put(paper_dict)
                    continue

                # 2) Error case: dict with structured error info
                if isinstance(result, dict):
                    if "Missing Doi Error" in result:
                        paper_dict["resolver_error"] = result.get("Missing Doi Error")
                    elif "url" in result:
                        pdf_url = result.get("url")
                        paper_dict["resolver_error"] = "Failed to Download PDF"
                        paper_dict["pdf_url"] = pdf_url
                    else:
                        paper_dict["resolver_error"] = "Unknown Resolver Error"

                    await unextracted_queue.put(paper_dict)
                    continue

                # 3) Success case: (pdf_bytes, pdf_url)
                pdf_bytes, pdf_url = result

                if pdf_bytes is None:
                    paper_dict["resolver_error"] = "No PDF Bytes Extracted"
                    await unextracted_queue.put(paper_dict)
                    continue

                print(f"Extracted paper {paper_doi}")
                paper_dict["pdf_bytes"] = pdf_bytes
                paper_dict["pdf_url"] = pdf_url
                await extracted_queue.put(paper_dict)

            except asyncio.TimeoutError:
                print(f"Async timeout fetching published PDF for {paper_id}")
                paper_dict["resolver_error"] = "Async Timeout Error"
                await unextracted_queue.put(paper_dict)
                continue

            except Exception as e:
                print(f"Error extracting published paper: {paper_id}: {e}")
                paper_dict["resolver_error"] = str(e)
                await unextracted_queue.put(paper_dict)
                continue

        finally:
            # This runs no matter what: success, continue, or exception
            task_queue.task_done()

async def writer(queue: asyncio.Queue, meta_path: Path, pdf_dir_path: Path | None = None) -> None:
    """
    Asynchronous consumer that writes items from a queue to a file
    in JSON Lines (.jsonl) format.

    Parameters
    ----------
    path : Path
        The path to the output `.jsonl` file. The file is opened in append mode.

    queue : asyncio.Queue
        The queue from which JSON-serializable items will be consumed.
        Each item should be a Python dictionary representing a record.

    Behavior
    --------
    - Runs in an infinite loop, pulling one item at a time from the queue.
    - When it receives `None`, it stops processing and exits cleanly.
    - Writes each item as a single JSON-formatted line to the file.
    - Used for writing both extracted and unextracted paper data.

    Notes
    -----
    - Only one writer should consume a given queue to avoid concurrent file writes.
    - This pattern ensures safe and efficient file output in an asyncio pipeline.
    """
    meta_path_obj = Path(meta_path)

    # Ensure pdf_dir_path is a Path if provided
    if pdf_dir_path is not None:
        pdf_dir_path = Path(pdf_dir_path)
        pdf_dir_path.mkdir(parents=True, exist_ok=True)


    while True:
        paper = await queue.get()
        try:
            if paper is None:
                print(f"Writer for {meta_path} shutting down.")
                return

             # If we have PDF bytes and a directory to save them
            if pdf_dir_path is not None and paper.get("pdf_bytes"):


                paper_id = paper["paper_id"]
                pdf_bytes = paper["pdf_bytes"]

                pdf_path = pdf_dir_path / f"{paper_id}.pdf"
                pdf_path.write_bytes(pdf_bytes)

                paper["pdf_path"] = str(pdf_path)
                paper.pop("pdf_bytes", None)  # Remove raw bytes before writing metadata
                print(f"Downloaded PDF for {paper_id} to {pdf_path}")


            async with aiofiles.open(meta_path_obj, "a", encoding="utf-8") as f:
                await f.write(json.dumps(paper) + "\n")

        except Exception as e:
            pid = paper.get("paper_id") if isinstance(paper, dict) else None
            print(f"Writer failed for {paper.get('paper_id')}: {e}")

        finally:
            queue.task_done()

            
            
async def main(): 
    """
    Orchestrates the entire asynchronous PDF extraction pipeline.

    Responsibilities
    ----------------
    1. Initializes three asyncio queues:
       - `task_q`: holds all tasks (one per paper to process)
       - `extracted_q`: receives successfully extracted papers
       - `unextracted_q`: receives papers that failed to resolve or extract

    2. Loads paper metadata from a CSV file into `task_q`.

    3. Starts multiple `extracting_pdf` workers to concurrently consume tasks
       from `task_q` and push results into either the extracted or unextracted queue.

    4. Launches one writer for each output file:
       - `writer(extracted_q)` → writes to `extracted_paper_info_thread.jsonl`
       - `writer(unextracted_q)` → writes to `unextracted_paper_info_thread.jsonl`

    5. Waits for all workers to complete, then signals the writers to shut down
       by pushing `None` (poison pills) into each queue.

    6. Awaits final completion of writer tasks to ensure all data is written.

    Notes
    -----
    - The number of concurrent workers is defined by `num_workers`.
    - The use of `None` as a shutdown signal ensures all tasks finish cleanly.
    - This function must be run inside an asyncio event loop (e.g., via `asyncio.run()`).
    """

    # Output queues for success and failure records
    extracted_q = asyncio.Queue()
    unextracted_q = asyncio.Queue()

    # Input queue with paper records to process
    task_q = asyncio.Queue()

    # Load CSV data into task queue
    await load_papers_from_csv(task_q)

    # Define number of concurrent workers
    num_workers = 7

    # Add one "poison pill" per worker to signal shutdown
    for _ in range(num_workers):
        await task_q.put(None)

    # Launch N parallel workers
    extract_tasks = [
        asyncio.create_task(extracting_pdf(task_q, extracted_q, unextracted_q, i))
        for i in range(num_workers)
    ]

    # Start asynchronous writers for each output file
    e_writer_task = asyncio.create_task(
    writer(
        extracted_q, 
        Path(extract_meta_path), 
        Path(pdf_file_dir)
        )
    )
    ue_writer_task = asyncio.create_task(
    writer(
        unextracted_q, 
        Path(unextracted_meta_path)
        )
    )

    # Wait for all worker tasks to complete
    await asyncio.gather(*extract_tasks)

    # Signal writers to shut down
    await extracted_q.put(None)
    await unextracted_q.put(None)

    # Wait for both writers to finish writing all data
    await asyncio.gather(e_writer_task, ue_writer_task)

    
if __name__ == "__main__":
    asyncio.run(main())
    print("Extraction completed. Check the output files for results.")

