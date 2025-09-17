"""
paper_extraction.py
===================

High‑level asyncio pipeline that:

1. Reads a CSV of article metadata (paper_id, title, DOI, cross‑ref link).
2. Resolves a **direct PDF URL** with :pyclass:`functions.pdf_resolver.PDFResolver`.
3. Extracts text with :pymeth:`functions.pdf_parser.extract_text_from_pdf_url`.
4. Streams results to two `.jsonl` files (extracted / unextracted) using
   producer–consumer queues and aiofiles.

Run it from the project root or any sub‑dir; paths are resolved from the
current working directory (``cwd``).

Usage
-----
```
python paper_extraction.py
```

Output
------
*  ``paper_extracts/extracted_paper_info/extracted_paper_info.jsonl``
   – records with usable PDF + text
*  ``paper_extracts/unextracted_paper_info/unextracted_paper_info.jsonl``
   – records we failed to download or parse
"""




import os, sys,asyncio,json
import aiofiles , aiocsv
cwd = os.getcwd()
parent_folder = os.path.abspath(os.path.join(cwd, ".."))
sys.path.append(parent_folder)
from typing import Optional
from pathlib import Path
from functions.extraction_functions import extract_text_with_pdf_resolver


paper_info_file = os.getenv("paper_info_file_path")


repo_root = Path.cwd().parent

extract_save_folder = os.path.join(repo_root, "paper_extracts", "extracted_paper_info")
unextracted_save_folder = os.path.join(repo_root, "paper_extracts", "unextracted_paper_info")


for folder in (extract_save_folder, unextracted_save_folder):
    os.makedirs(folder, exist_ok=True)
    
    
extract_file_path = os.path.join(extract_save_folder, "extracted_paper_info.jsonl")
unextracted_file_path = os.path.join(unextracted_save_folder, "unextracted_paper_info.jsonl")



async def extracting_pdf(extracted_queue: asyncio.Queue,unextracted_queue: asyncio.Queue,) -> None:
    """Producer: iterate CSV rows → resolve PDF → extract text → enqueue."""

    async with aiofiles.open(paper_info_file,'r') as f:
        reader = aiocsv.AsyncReader(f)
        header = await reader.__anext__()
        async for row in reader:
            pdf_url: Optional[str] = None
            paper_id, paper_title, paper_doi, paper_link = row[0], row[1], row[2], str(row[3])
            paper_dict = {
            "doi": paper_doi,
            "title":paper_title,
            "crossref_paper_link": paper_link,
            "id":paper_id,
            "pdf_url":"",
            "paper_text":"", 
            "resolver_error":""
        }
            pdf_text = None
            pdf_url = None
            try:
                pdf_text = await extract_text_with_pdf_resolver(doi = paper_doi, paper_id= paper_id , selector_timeout=40000)
                if pdf_text is None: 
                    paper_dict['resolver_error'] = "Unknown Resolver Error"
                    await unextracted_queue.put(paper_dict)
                    continue
                if isinstance(pdf_text, dict): 
                    if "Missing Doi Error" in pdf_text:
                        paper_dict['resolver_error'] = pdf_text.get("Missing Doi Error")
                        paper_dict['pdf_url'] = pdf_url
                        paper_dict['paper_text'] = pdf_text
                        await unextracted_queue.put(paper_dict)
                        continue
                    elif "url" in pdf_text: 
                        pdf_url = pdf_text.get("url")
                        paper_dict['resolver_error'] = "Failed to Download PDF"
                        paper_dict['url'] = pdf_url
                        pdf_text = None
                        await unextracted_queue.put(paper_dict)
                        continue
            except  asyncio.TimeoutError as e:
                print("Async timeout fetching published PDF for %s: %s", paper_id) # change to log
                paper_dict['resolver_error'] = "Async Timeout Error"
            except Exception as e:
                print(f"Error extracting published paper: {paper_id}")
                paper_dict['resolver_error'] = str(e)
                await unextracted_queue.put(paper_dict)
                continue
            if pdf_text: 
                print(f"Extracted paper {paper_doi}")
                paper_dict.update({
                "paper_text":pdf_text
                })
                
                await extracted_queue.put(paper_dict)



async def writer(path: Path, queue: asyncio.Queue) -> None:
    """Consumer: write each JSON serialised item from *queue* to *path*."""

    async with aiofiles.open(path, 'a') as f:
        while True:
            item = await queue.get()
            if item is None:
                break
            await f.write(json.dumps(item) + "\n")



async def main(): 
    
    """Kick off producer + two writer tasks and wait for completion."""

    extracted_q = asyncio.Queue()
    unextracted_q = asyncio.Queue()
    
    
        
    extract_task = asyncio.create_task(
        extracting_pdf(extracted_q , unextracted_q)
    )
    
    e_writer_task = asyncio.create_task(writer(extract_file_path, extracted_q))
    ue_writer_task = asyncio.create_task(writer(unextracted_file_path, unextracted_q))
    
    await extract_task
    await extracted_q.put(None)
    await unextracted_q.put(None)
    
    await asyncio.gather(e_writer_task, ue_writer_task)
    

if __name__ == "__main__":
    asyncio.run(main())
    print("Extraction completed. Check the output files for results.")
