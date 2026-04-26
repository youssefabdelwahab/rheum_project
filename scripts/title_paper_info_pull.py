import json
import argparse
import asyncio
import logging
from datetime import date
from pathlib import Path
from modules.paper_to_doi import get_article_info_from_title

logger = logging.getLogger(__name__)


def parse_args(): 
    ap = argparse.ArgumentParser(description="Get PDF Links from crossref")
    ap.add_argument('--dir', type=Path,required=True, help='dir of the file with the doi of the papers - will also be where the output file will be saved')
    ap.add_argument('--paper_file', type=Path, required=True , help='path of the file')
    ap.add_argument('--log_dir', type=Path, required=False, default=Path('./localworkspace'),help='Default will be ./localworkspace')
    return ap.parse_args()



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
        logger.info(f"Processing paper: {row.get('recordid.')}")
        cleaned_title = extract_title_and_info(row.get('citation'))
        if not cleaned_title: 
            logger.info("Could not extract title from citation")
            result = {
            'paper_id': row.get('recordid.'),
            'cross_ref_paper_title': paper_info.get('title', ''),
            'cross_ref_paper_doi': paper_info.get('doi', ''),
            'cross_ref_paper_link': paper_info.get('document_link', ''),
            'paper_citation': row.get('citation'),
            'paper_abstract' : row.get('abstract'),
            'error': 'Could Not extract title from citation'
        }
            
        paper_info = await asyncio.to_thread(get_article_info_from_title, str(cleaned_title)) or {}
        if paper_info:
            logger.info(f"Found Information for paper {row[0]}")
            result = {
            'paper_id': row.get('recordid.'),
            'cross_ref_paper_title': paper_info.get('title', ''),
            'cross_ref_paper_doi': paper_info.get('doi', ''),
            'cross_ref_paper_link': paper_info.get('document_link', ''),
            'paper_citation': row.get('citation'),
            'paper_abstract' : row.get('abstract'),
            'error': 'No Error'
        }

        else:

            logging.info(f"No information found for paper: {row.get('recordid.')}")
            result = {
            'paper_id': row.get('recordid.'),
            'cross_ref_paper_title': paper_info.get('title', ''),
            'cross_ref_paper_doi': '',
            'cross_ref_paper_link': '',
            'paper_citation': row.get('citation'),
            'paper_abstract' : row.get('abstract'),
            'error': 'No Information found'
        }
        
        await queue.put(result)
        
    await queue.put(None)  # Signal that processing is done
    
    
async def json_writer(file_path, queue): 
    with open(file_path, 'w', encoding='utf-8') as new_file:
        
        while True:
            row = await queue.get()
            if row is None:
                break
                
            json_string = json.dumps(row, ensure_ascii=False)
            new_file.write(json_string + '\n')
            
            
            
async def main():

    args = parse_args()
    file_dir = args.dir
    file_path = file_dir / args.paper_file
    result_path = file_dir / f"paper_journal_info_{date.today():%Y-%m-%d}.json"


    args.log_dir.mkdir(parents=True, exist_ok=True)
    log_file_path = args.log_dir / f"paper_info_extraction_{date.today():%Y-%m-%d}.log"


    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file_path),
            logging.StreamHandler() 
        ]
    )
    
    logger.info("Loaded Env File successfully.")
    logger.info(f"Starting to process papers from {file_path}")

    with open(file_path, 'r', encoding='utf-8') as paper_file: 
        for line in paper_file: 
            row_data = json.loads(line)
    
    queue = asyncio.Queue()
    
    await asyncio.gather(
        pulling_info(row_data, queue),
        json_writer(result_path, queue)
    )

    logger.info(f"Finished processing. Results saved to {result_path}")

if __name__ == "__main__":
    asyncio.run(main())