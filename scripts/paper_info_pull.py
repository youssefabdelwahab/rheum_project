import pandas as pd 
import os 
import sys
import csv
import asyncio
from pathlib import Path


sys.path.append(os.path.abspath('..'))
from modules.paper_to_doi import get_article_info_from_title

from dotenv import load_dotenv
load_dotenv()

env_path = "/work/robust_ai_lab/shared/env_vars/rheum_project/env_vars.sh" # export SCRIPT_ENV_FILE=/full/path/to/env_vars.sh
if not env_path:
        raise RuntimeError("SCRIPT_ENV_FILE is not set")

env_path = str(Path(env_path).expanduser())
ok = load_dotenv(dotenv_path=env_path, override=False)
if not ok:
    raise FileNotFoundError(f"Could not load env file at {env_path}")
print("Loaded Env File")


def extract_title_and_info(citation: str) -> str:
    # Split the citation by period
    parts = [p.strip() for p in citation.split('.') if p.strip()]
    
    # Expect: [authors, title, journal/info, ...]
    if len(parts) >= 3:
        # Join title and journal/info
        return f"{parts[1]}. {parts[2]}.{parts[3]}"
    elif len(parts) == 2:
        # If no journal info, return just the title
        return parts[1]
    else:
        return ""




async def pulling_info (row_data , queue): 
    
    for row in row_data: 
        print(f"Processing paper: {row[2]}")
        cleaned_title = extract_title_and_info(row[2])
        paper_info = await asyncio.to_thread(get_article_info_from_title, str(cleaned_title)) or {}
        if not paper_info:
            print(f"No information found for paper: {row[2]}")
            continue
        
        result = {
            'paper_id': row[0],
            'cross_ref_paper_title': paper_info.get('title', ''),
            'cross_ref_paper_doi': paper_info.get('doi', ''),
            'cross_ref_paper_link': paper_info.get('document_link', '')
        }
        await queue.put(result)
        
    await queue.put(None)  # Signal that processing is done
    
    
async def csv_writer(file_path , queue): 
    
    first_row = await queue.get()
    if first_row is None:
        return
    
    with open(file_path, 'w', newline='' , encoding='utf-8') as new_file:
        writer = csv.DictWriter(new_file, fieldnames=first_row.keys())
        writer.writeheader()
        writer.writerow(first_row)
        
        while True:
            row = await queue.get()
            if row is None:
                break
            writer.writerow(row)
            
            
            
async def main():


    papers_dir = os.getenv("PAPER_DATABASE_PATH")
    research_paper_database = os.path.join(papers_dir, "Living database of RA trials September 2024_1_2_final_withCRSID_gh.xlsx")
    paper_info_path = os.path.join(papers_dir, f"paper_journal_info{datetime.now():%Y-%m-%d}.csv")

    with open(research_paper_database, newline='') as paper_database:
        reader = csv.reader(paper_database)
        next(reader)
        row_data = list(reader)
    
    queue = asyncio.Queue()
    
    await asyncio.gather(
        pulling_info(row_data, queue),
        csv_writer(paper_info_path, queue)
    )

        
if __name__ == "__main__":
    asyncio.run(main())