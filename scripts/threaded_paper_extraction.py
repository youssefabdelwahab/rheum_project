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
load_dotenv()

from dotenv import load_dotenv

env_path = "/work/robust_ai_lab/shared/env_vars/rheum_project/env_vars.sh"  # export SCRIPT_ENV_FILE=/full/path/to/env_vars.sh
if not env_path:
    raise RuntimeError("SCRIPT_ENV_FILE is not set")

env_path = str(Path(env_path).expanduser())
ok = load_dotenv(dotenv_path=env_path, override=False)
if not ok:
    raise FileNotFoundError(f"Could not load env file at {env_path}")
print("Loaded Env File")

papers_dir = os.getenv("PAPER_DATABASE_PATH")
paper_info_file = os.path.join(papers_dir, "paper_journal_info.csv")


extract_save_folder = os.path.join(papers_dir, "extracted")
unextracted_save_folder = os.path.join(papers_dir, "unextracted")


for folder in (extract_save_folder, unextracted_save_folder):
    os.makedirs(folder, exist_ok=True)
    
    
extract_file_path = os.path.join(extract_save_folder, "extracted_paper_info_thread.jsonl")
unextracted_file_path = os.path.join(unextracted_save_folder, "unextracted_paper_info_thread.jsonl")


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

        # Read each data row and add it to the task queue
        async for row in reader:
            paper_id = row[0]
            paper_title = row[1]
            paper_doi = row[2]
            paper_link = str(row[3])  # ensure it's a string

            # Create a task dictionary to be processed later
            task_queue.put_nowait({
                "doi": paper_doi,
                "title": paper_title,
                "crossref_paper_link": paper_link,
                "id": paper_id,
                "pdf_url": "",           # will be filled by resolver
                "paper_text": "",        # will be filled after extraction
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
        if paper_dict is None:
            print(f"Worker {worker_id} finished processing.")
            break  # Shut down the worker gracefully

        paper_doi = paper_dict.get("doi")
        paper_id = paper_dict.get("id")
        pdf_text = None
        pdf_url = None

        try:
            # Attempt to resolve PDF and extract text
            pdf_text = await extract_text_with_pdf_resolver(
                doi=paper_doi,
                paper_id=paper_id,
                selector_timeout=40000
            )

            # Handle case where resolver returned nothing
            if pdf_text is None:
                paper_dict['resolver_error'] = "Unknown Resolver Error"
                await unextracted_queue.put(paper_dict)
                continue

            # Handle structured error response (dictionary)
            if isinstance(pdf_text, dict):
                # Specific case: DOI was not found
                if "Missing Doi Error" in pdf_text:
                    paper_dict['resolver_error'] = pdf_text.get("Missing Doi Error")
                    paper_dict['pdf_url'] = pdf_url
                    paper_dict['paper_text'] = pdf_text
                    await unextracted_queue.put(paper_dict)
                    continue

                # Specific case: resolver couldn't download from the landing page
                elif "url" in pdf_text:
                    pdf_url = pdf_text.get("url")
                    paper_dict['resolver_error'] = "Failed to Download PDF"
                    paper_dict['url'] = pdf_url
                    await unextracted_queue.put(paper_dict)
                    continue

        except asyncio.TimeoutError:
            # Handle network timeout
            print(f"Async timeout fetching published PDF for {paper_id}")
            paper_dict['resolver_error'] = "Async Timeout Error"
            await unextracted_queue.put(paper_dict)
            continue

        except Exception as e:
            # Handle unexpected exceptions
            print(f"Error extracting published paper: {paper_id}")
            paper_dict['resolver_error'] = str(e)
            await unextracted_queue.put(paper_dict)
            continue

        # If successful, store the extracted text
        if pdf_text:
            print(f"Extracted paper {paper_doi}")
            paper_dict["paper_text"] = pdf_text
            await extracted_queue.put(paper_dict)


async def writer(path: Path, queue: asyncio.Queue) -> None:
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

    async with aiofiles.open(path, 'a') as f:
        while True:
            item = await queue.get()

            # Special signal to shut down the writer
            if item is None:
                break

            # Serialize to JSON and write a single line
            await f.write(json.dumps(item) + "\n")

            
            
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
    e_writer_task = asyncio.create_task(writer(extract_file_path, extracted_q))
    ue_writer_task = asyncio.create_task(writer(unextracted_file_path, unextracted_q))

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

