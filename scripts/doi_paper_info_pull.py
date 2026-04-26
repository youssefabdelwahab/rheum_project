
import argparse
import asyncio
from pathlib import Path
from datetime import date
import logging
import json
from modules.paper_to_doi import get_info_from_doi

logger = logging.getLogger(__name__)




def parse_args(): 
    ap = argparse.ArgumentParser(description="Get PDF Links from crossref")
    ap.add_argument('--dir', type=Path,required=True, help='dir of the file with the doi of the papers - will also be where the output file will be saved')
    ap.add_argument('--paper_file', type=Path, required=True , help='path of the file')
    ap.add_argument('--log_dir', type=Path, required=False, default=Path('./localworkspace'),help='Default will be ./localworkspace')
    return ap.parse_args()

async def pulling_info(row_data , queue): 
    for row in row_data: 
        logger.info(f"Processing paper: {row.get('recordid.')}")
        doi =  str(row.get('DOI')) 
        if not doi: 
            logger.info(f'No DOI for paper {row.get('recordid.')}')
            result = {
            'paper_id': row.get('recordid.'),
            'cross_ref_paper_title': paper_info.get('title', ''),
            'paper_citation': row.get('citation'),
            'paper_abstract' : row.get('abstract'),
            'cross_ref_paper_doi': paper_info.get('doi', ''),
            'cross_ref_paper_link': paper_info.get('document_link', ''),
            'cross_ref_paper_license': paper_info.get('document_link', ''),
            'error' : "No DOI"
        }
        paper_info = await asyncio.to_thread(get_info_from_doi, str(doi)) or {}
        if paper_info: 
            logger.info(f"Found Information for paper {row.get('recordid.')} ")
            result = {
                'paper_id': row.get('recordid.'),
                'cross_ref_paper_title': paper_info.get('title', ''),
                'paper_citation': row.get('citation'),
                'paper_abstract' : row.get('abstract'),
                'cross_ref_paper_doi': paper_info.get('doi', ''),
                'cross_ref_paper_link': paper_info.get('document_link', ''),
                'cross_ref_paper_license': paper_info.get('document_link', ''),
                'error' : "No Error"
            }
        else: 
           logger.info(f"No information found for paper: {row.get('recordid.')}")
           result = {
                'paper_id': row.get('recordid.'),
                'cross_ref_paper_title': '',
                'paper_citation': row.get('citation'),
                'paper_abstract': row.get('abstract'),
                'cross_ref_paper_doi': '',
                'cross_ref_paper_link': '',
                'cross_ref_paper_license': '',
                'error' : f"No information found for paper: {row.get('recordid.')}"
            }
        await queue.put(result)
    await queue.put(None) 



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