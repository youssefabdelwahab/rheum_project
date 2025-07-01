import pandas as pd 
import os 
import sys
import csv
import asyncio
sys.path.append(os.path.abspath('..'))
from modules.paper_to_doi import get_article_info_from_title
from dotenv import load_dotenv
load_dotenv()



async def pulling_info (row_data , queue): 
    
    for row in row_data: 
        print(f"Processing paper: {row[2]}")
        paper_info = await asyncio.to_thread(get_article_info_from_title, str(row[2])) or {}
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

    research_paper_info_file = os.getenv("paper_info_file_path")
    database_file= os.getenv("paper_database_file_path")
    
    with open(database_file, newline='') as paper_database:
        reader = csv.reader(paper_database)
        next(reader)
        row_data = list(reader)
    
    queue = asyncio.Queue()
    
    await asyncio.gather(
        pulling_info(row_data, queue),
        csv_writer(research_paper_info_file, queue)
    )

        
if __name__ == "__main__":
    asyncio.run(main())