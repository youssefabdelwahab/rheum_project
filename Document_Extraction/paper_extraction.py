import os, sys,asyncio,json
import aiofiles , aiocsv
cwd = os.getcwd()
parent_folder = os.path.abspath(os.path.join(cwd, ".."))
sys.path.append(parent_folder)
from functions import pdf_resolver
import functions.pdf_parser as pdf_parser


paper_info_file = "../research_paper_database/paper_journal_info.csv"


repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

extract_save_folder = os.path.join(repo_root, "paper_extracts", "extracted_paper_info")
unextracted_save_folder = os.path.join(repo_root, "paper_extracts", "unextracted_paper_info")


for folder in (extract_save_folder, unextracted_save_folder):
    os.makedirs(folder, exist_ok=True)
    
    
extract_file_path = os.path.join(extract_save_folder, "extracted_paper_info.jsonl")
unextracted_file_path = os.path.join(unextracted_save_folder, "unextracted_paper_info.jsonl")


async def extracting_pdf(extracted_queue , unextracted_queue):
    async with aiofiles.open(paper_info_file,'r') as f:
        reader = aiocsv.AsyncReader(f)
        header = await reader.__anext__()
        async for row in reader:
            paper_id = row[0]
            paper_title = row[1]
            paper_doi = row[2]
            paper_link = row[3] 
        paper_dict = {
            "doi": paper_doi,
            "title":paper_title,
            "crossref_paper_link": paper_link or None,
            "id":paper_id,
            "pdf_url":"",
            "paper_text":""
        }
            # need to adjust what to do if there is no paper_doi ,
            # is the paper link viable to extract the pdf from?
        async with pdf_resolver.PDFResolver() as resolver:
            try:
                pdf_url = await resolver.get_pdf(paper_doi)
                text = await pdf_parser.extract_text_from_pdf_url(pdf_url)
                if text: 
                    paper_dict.update({
                    "pdf_url":pdf_url, 
                    "paper_text":text
                    })
                    await extracted_queue.put(paper_dict)
            
            except resolver.CantDownload as e: 
                print(f"PDF could not be downloaded for DOI {paper_doi}: {e} ... storing landing_url")
            finally: 
                print('Storing landing url for manual download')
                paper_dict.update({
                    "pdf_url":pdf_url
                })
                await unextracted_queue.put(paper_dict)
            
    


async def writer(path, queue):
    async with aiofiles.open(path, 'a') as f:
        while True:
            item = await queue.get()
            if item is None:
                break
            await f.write(json.dumps(item) + "\n")



async def main(): 
    
    
    extracted_q = asyncio.Queue()
    unextracted_q = asyncio.Queue()
    
    extract_task = asyncio.create_task(
        extracting_pdf(extracted_q , unextracted_q)
    )
    
    e_writer_task = asyncio.create_task(writer(extract_save_folder, extracted_q))
    ue_writer_task = asyncio.create_task(writer(unextracted_save_folder, unextracted_q))
    
    await extract_task
    await extracted_q.put(None)
    await unextracted_q.put(None)
    
    await asyncio.gather(e_writer_task, ue_writer_task)
    

if __name__ == "__main__":
    asyncio.run(main())
    print("Extraction completed. Check the output files for results.")
