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




import os, sys,asyncio,json, csv
import aiofiles , aiocsv
# cwd = os.getcwd()
# parent_folder = os.path.abspath(os.path.join(cwd, ".."))
# sys.path.append(parent_folder)

## Import utils from utils folder
current_dir = os.path.dirname(os.path.abspath(__file__))
print(current_dir)
# Check if utils is a sibling folder (local dev)
utils_path_local = os.path.abspath(os.path.join(current_dir, '..'))
sys.path.append(utils_path_local)

from typing import Optional
from pathlib import Path
from functions import pdf_resolver 
import functions.pdf_parser as pdf_parser


paper_info_file = "research_paper_database/paper_journal_info.csv"


repo_root = Path.cwd().parent

extract_save_folder = os.path.join(repo_root, "paper_extracts", "extracted_paper_info")
unextracted_save_folder = os.path.join(repo_root, "paper_extracts", "unextracted_paper_info")


for folder in (extract_save_folder, unextracted_save_folder):
    os.makedirs(folder, exist_ok=True)
    
    
extract_file_path = os.path.join(extract_save_folder, "extracted_paper_info.jsonl")
unextracted_file_path = os.path.join(unextracted_save_folder, "unextracted_paper_info.jsonl")

extract_csv_path = os.path.join(extract_save_folder, "extracted_paper_info.csv")
unextracted_csv_path = os.path.join("unextracted_paper_info.csv")


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
        
            async with pdf_resolver.PDFResolver() as resolver:
                try:
                    pdf_url = await resolver.get_pdf(paper_doi, paper_link, paper_id)
                
                except resolver.MissingIdentifier as e: 
                    print("No DOI or Paper Link available for paper, skipping extraction")
                    paper_dict.update({
                    "resolver_error": str(e)
                    })
                    await unextracted_queue.put(paper_dict)
                    continue
                except resolver.CantDownload as e: 
                    print(f"PDF could not be downloaded for DOI {paper_doi}: {e} ... storing landing_url")
                    paper_dict.update({
                    "pdf_url":e.landing,
                "   resolver_error": str(e)
                    })
                    await unextracted_queue.put(paper_dict)
                    continue
            
                except Exception as e:
                    print(f"Error extracting paper {e}")
                    paper_dict.update({
                "pdf_url":pdf_url,
                "resolver_error": str(e)
                })
                    await unextracted_queue.put(paper_dict)
                    continue
                else:
                    text = await pdf_parser.extract_text_from_pdf_url(str(pdf_url))
                    if not text: 
                        paper_dict.update({
                    "pdf_url": pdf_url
                    })
                        await unextracted_queue.put(paper_dict)
                        continue
                    print(f"Extracted paper {paper_doi}")
                    paper_dict.update({
                    "pdf_url":pdf_url,
                    "paper_text": text
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

async def writer_csv(path: Path, queue: asyncio.Queue, fieldnames: list[str]) -> None:
    """Write each item from the queue to a csv file."""
    write_header = not os.path.exists(path)    
    async with aiofiles.open(path, 'a', newline = '') as f:
        writer = None
        while True:
            item = await queue.get()
            if item is None:
                break

            line = ','.join('"{}"'.format(str(item.get(key, "")).replace('"','""')) for key in fieldnames)

            if write_header:
                header_line = ','.join(f'"{h}"' for h in fieldnames)
                await f.write(header_line + '\n')
                write_header = False

            await f.write(line + '\n')

async def main(): 
    
    """Kick off producer + two writer tasks and wait for completion."""

    extracted_q = asyncio.Queue()
    unextracted_q = asyncio.Queue()
          
    extract_task = asyncio.create_task(
        extracting_pdf(extracted_q , unextracted_q)
    )
    
    e_writer_task = asyncio.create_task(writer(extract_file_path, extracted_q))
    ue_writer_task = asyncio.create_task(writer(unextracted_file_path, unextracted_q))

    csv_fields = ["id", "title", "doi", "crossref_paper_link", "pdf_url", "paper_text", "resolver_error"]
    e_writer_csv = asyncio.create_task(writer_csv(extract_csv_path, extracted_q, csv_fields))
    ue_writer_csv = asyncio.create_task(writer_csv(unextracted_csv_path, unextracted_q, csv_fields))
    
    await extract_task
    for q in (extracted_q, unextracted_q):
        await q.put(None)
        await q.put(None)
    
    await asyncio.gather(e_writer_task, ue_writer_task, e_writer_csv, ue_writer_csv)
    

if __name__ == "__main__":
    asyncio.run(main())
    print("Extraction completed. Check the output files for results.")
