# import os
# import sys
import asyncio
import json
import random
from datetime import date
import re
import aiofiles
from pathlib import Path
import argparse
import logging
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from curl_cffi.requests import AsyncSession
from dotenv import load_dotenv
load_dotenv()

logger = logging.getLogger(__name__)




def parse_args(): 
    ap = argparse.ArgumentParser(description="Download Paper PDFS")
    ap.add_argument('--dir', required=True, type=Path, help='File Dir')
    ap.add_argument('--paper_meta_file' , required=True, type=Path, help='input file that contains paper links')
    ap.add_argument('--pdf_save_dir' , required=True, type=Path, help='Dir to Save Downloaded PDFS')
    ap.add_argument("--num_workers" , required=True, type=int, help= 'Worker Allocation')
    ap.add_argument('--log_dir', type=Path, required=False, default=Path('./localworkspace'),help='Default will be ./localworkspace')
    return ap.parse_args()



# Common headers to help with access on some academic sites
HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5", 
    "Cache-Control": "no-cache",
}

# PAPER_DOWNLOAD_LOGGING_DICT = { 
#     "Paper_Id":'',
#     "url_1":'',
#     "url_2":'',
#     "PDF_Extracted":'',
#     "Error":''
# }



def find_pdf_link_in_html(html_content, base_url):
    try:
        soup = BeautifulSoup(html_content, 'html.parser')

        # STRATEGY 1: Meta tags
        meta_tag = soup.find('meta', attrs={'name': 'citation_pdf_url'})
        if meta_tag and meta_tag.get('content'):
            return urljoin(base_url, meta_tag['content'])

        # STRATEGY 2: Iframes
        for iframe in soup.find_all('iframe', src=True):
            src = iframe['src']
            if '.pdf' in src.lower() or 'pdf' in src.lower():
                return urljoin(base_url, src)

        # STRATEGY 3: Buttons
        for a_tag in soup.find_all('a', href=True):
            text = a_tag.get_text().lower().strip()
            href = a_tag['href']
            if text in ['open', 'open pdf', 'download', 'download pdf']:
                 return urljoin(base_url, href)

        # STRATEGY 4: Links ending in .pdf
        for a_tag in soup.find_all('a', href=True):
            href = a_tag['href'].strip()
            if href.lower().endswith('.pdf'):
                return urljoin(base_url, href)
            if 'pdf' in a_tag.get_text().lower() and 'full' in a_tag.get_text().lower():
                 return urljoin(base_url, href)

    except Exception as e:
        print(f"[!] Error parsing HTML: {e}")
    return None

def get_smart_pdf_url(landing_url):
    """
    Predicts the PDF link based on the URL structure.
    Bypasses the need to scrape buttons on JavaScript-heavy sites.
    """
    landing_url = str(landing_url) # Safety cast

    # 1. ScienceDirect / Elsevier (The "PII" Trick)
    # We look for the pattern /pii/S... followed by 16 chars (digits or X)
    if "sciencedirect.com" in landing_url or "linkinghub.elsevier.com" in landing_url:
        # Regex to capture the ID starting with 'S' followed by numbers/X
        # It stops at the first non-alphanumeric char (like ? or /)
        pii_match = re.search(r'/pii/(S[0-9A-Z]{16})', landing_url, re.IGNORECASE)
        
        if pii_match:
            pii_id = pii_match.group(1)
            print(f"    [*] Detected ScienceDirect PII: {pii_id}")
            return f"https://www.sciencedirect.com/science/article/pii/{pii_id}/pdfft"

    # 2. Wiley (Force PDF Direct)
    if "onlinelibrary.wiley.com" in landing_url and "/doi/" in landing_url:
        # Normalize various Wiley URL formats to 'pdfdirect'
        if "/doi/pdf/" in landing_url:
             landing_url = landing_url.replace("/doi/pdf/", "/doi/pdfdirect/")
        elif "/doi/full/" in landing_url:
             landing_url = landing_url.replace("/doi/full/", "/doi/pdfdirect/")
        elif "/doi/epdf/" in landing_url:
             landing_url = landing_url.replace("/doi/epdf/", "/doi/pdfdirect/")
        elif "/doi/" in landing_url and "pdfdirect" not in landing_url:
             # Be careful not to break non-article URLs, but usually safe for papers
             landing_url = landing_url.replace("/doi/", "/doi/pdfdirect/")
             
        if "?download=true" not in landing_url:
            landing_url += "?download=true"
        return landing_url

    # 3. MDPI (Version Fix)
    if "mdpi.com" in landing_url and landing_url.endswith("/pdf"):
        if "?version=" not in landing_url:
             return f"{landing_url}?version=1"

    return None



# --- 3. ASYNC DOWNLOAD LOGIC ---

async def download_one_paper(paper_info):
    """
    The core logic from your first script, converted to Asyncio.
    """
    paper_id = paper_info.get("paper_id")
    raw_urls = [paper_info.get('url_1'), paper_info.get('url_2')]
    urls_to_try = [u for u in raw_urls if u]

    log_dict = {
        "Paper_Id": paper_id,
        "url_1": raw_urls[0] if raw_urls and len(raw_urls) > 0 else "",
        "url_2": raw_urls[1] if raw_urls and len(raw_urls) > 1 else "",
        "PDF_Extracted": False,
        "Error": ""
    }

    if not urls_to_try:
        paper_info["error"] = "No URLs provided"
        log_dict["Error"] = "No URLs provided"
        logging.info(log_dict)
        return None, paper_info
    
    # We use AsyncSession for non-blocking HTTP requests
    async with AsyncSession() as session:
        error = "No specific error captured"
        
        # Priority: Edge -> Chrome -> Safari
        masks = ["edge101", "chrome120", "safari15_3"]

        for url in urls_to_try:
            # --- PRE-FLIGHT FIX: Domain Correction ---
            if "linkinghub.elsevier.com" in url and "/pii/" in url:
                try:
                    pii_id = url.split('/pii/')[-1].split('?')[0].split('/')[0]
                    url = f"https://www.sciencedirect.com/science/article/pii/{pii_id}"
                except:
                    pass

            for mask in masks:
                try:
                    # Random sleep to prevent being IP banned (essential even in async)
                    await asyncio.sleep(random.uniform(1, 3))
                    
                    # print(f"[*] [{paper_id}] Visiting {url} (Mask: {mask})...")
                    
                    # STEP A: VISIT LANDING PAGE
                    req_headers = HEADERS.copy()
                    req_headers["Referer"] = "https://www.google.com/"
                    
                    landing_resp = await session.get(
                        url, 
                        headers=req_headers, 
                        impersonate=mask, 
                        timeout=30, 
                        allow_redirects=True
                    )
                    
                    if landing_resp.status_code == 403:
                        # print(f"    [!] [{paper_id}] 403 on landing. Rotating mask...")
                        log_dict["Error"] = landing_resp.status_code
                        logger.info(log_dict)
                        continue 

                    final_landing_url = landing_resp.url
                    
                    # Check for direct PDF download
                    if 'application/pdf' in landing_resp.headers.get('Content-Type', '').lower():
                        print(f"[+] [{paper_id}] Success (Direct)")

                        return landing_resp.content, paper_info

                    # STEP B: FIND THE PDF LINK
                    pdf_target_url = None
                    landing_text = landing_resp.text # Access text property once

                    # CASE 1: ScienceDirect
                    if "sciencedirect.com" in final_landing_url:
                        # Prioritize Meta Tag
                        scraped_link = find_pdf_link_in_html(landing_text, final_landing_url)
                        if scraped_link:
                            pdf_target_url = scraped_link
                        else:
                            pdf_target_url = get_smart_pdf_url(final_landing_url)

                    # CASE 2: Wiley
                    elif "onlinelibrary.wiley.com" in final_landing_url:
                        # Prioritize Smart Link
                        pdf_target_url = get_smart_pdf_url(final_landing_url)
                        if not pdf_target_url:
                            pdf_target_url = find_pdf_link_in_html(landing_text, final_landing_url)

                    # CASE 3: Others
                    else:
                        pdf_target_url = find_pdf_link_in_html(landing_text, final_landing_url)
                        if not pdf_target_url:
                            pdf_target_url = get_smart_pdf_url(final_landing_url)
                    
                    if not pdf_target_url:
                        error = "HTML loaded, no PDF link found"
                        break # Page loaded fine, but empty. Don't retry masks.

                    # STEP C: DOWNLOAD PDF
                    pdf_headers = HEADERS.copy()
                    pdf_headers["Referer"] = final_landing_url 
                    
                    pdf_resp = await session.get(
                        pdf_target_url,
                        headers=pdf_headers,
                        impersonate=mask,
                        timeout=45
                    )

                    content_type = pdf_resp.headers.get('Content-Type', '').lower()

                    if pdf_resp.status_code == 200 and 'application/pdf' in content_type:
                        print(f"[+] [{paper_id}] Success (Extracted)")
                        # paper_download_logging_dict.update(
                        #         Paper_Id=paper_id, 
                        #         url_1=raw_urls[0] if raw_urls[0] else "",
                        #         url_2=raw_urls[1] if raw_urls[1] else "",
                        #         PDF_Extracted=True,
                        #         Error=error
            
                        # )
                        # logger.info(paper_download_logging_dict)
                        return pdf_resp.content, paper_info
                    
                    # RECOVERY: Backup Scrape
                    elif 'text/html' in content_type and "sciencedirect" not in final_landing_url:
                        backup_link = find_pdf_link_in_html(landing_text, final_landing_url)
                        if backup_link and backup_link != pdf_target_url:
                            pdf_resp = await session.get(backup_link, headers=pdf_headers, impersonate=mask)
                            if pdf_resp.status_code == 200 and 'application/pdf' in pdf_resp.headers.get('Content-Type', '').lower():
                                print(f"[+] [{paper_id}] Success (Backup)")
                                # paper_download_logging_dict.update(
                                # Paper_Id=paper_id, 
                                # url_1=raw_urls[0] if raw_urls[0] else "",
                                # url_2=raw_urls[1] if raw_urls[1] else "",
                                # PDF_Extracted=True,
                                # Error=error
            
                                #     )
                                # logger.info(paper_download_logging_dict)
                                return pdf_resp.content, paper_info

                    if pdf_resp.status_code == 403:
                        # print(f"    [!] [{paper_id}] 403 on PDF. Rotating mask...")
                        log_dict["Error"] = "403 Forbidden on PDF target"
                        logger.info(log_dict)
                        continue 
                    
                    else:
                        error = f"PDF req failed ({pdf_resp.status_code})"
                        log_dict["Error"] = error
                        logger.info(log_dict)
                        break 

                except Exception as e:
                    # print(f"[!] [{paper_id}] Error: {e}")
                    error = str(e)
                    logger.info(error)
        
        paper_info["error"] = error
        return None, paper_info
    

# --- 4. PIPELINE COMPONENTS (Producer/Consumer/Worker) ---

async def load_papers_from_jsonl(input_file: Path, task_queue: asyncio.Queue):
    """
    Reads JSONL input and fills the queue.
    """
    if not input_file.exists():
        print(f"Input file not found: {input_file}")
        return

    async with aiofiles.open(input_file, 'r', encoding='utf-8') as f:
        seen_ids = set()
        async for line in f:
            line = line.strip()
            if not line: continue
            
            try:
                paper_data = json.loads(line)
                paper_id = paper_data.get('paper_id')
                
                # Deduplication logic (optional)
                if paper_id in seen_ids:
                    continue
                seen_ids.add(paper_id)

                # Normalize input for the worker
                task_item = {
                    "paper_id": paper_id,
                    "url_1": paper_data.get('cross_ref_paper_link'),
                    "url_2": paper_data.get('cross_ref_paper_license') 
                }
                
                await task_queue.put(task_item)
                
            except json.JSONDecodeError:
                pass
    
    logger.info("[Loader] Finished loading all tasks into queue.")


async def worker_pdf_downloader(pdf_fir:Path,
                                task_queue: asyncio.Queue, 
                                extracted_queue: asyncio.Queue, 
                                unextracted_queue: asyncio.Queue, 
                                worker_id: int):
    """
    Consumes tasks, runs the download logic, and sorts results.
    """
    while True:
        paper_info = await task_queue.get()
        
        # Poison pill check
        if paper_info is None:
            task_queue.task_done()
            break
        
        try:
            # Check if file already exists before processing
            paper_id = paper_info.get("paper_id")
            target_file = pdf_fir / f"{paper_id}.pdf"
            
            if target_file.exists():
                logger.info(f"[Worker {worker_id}] PDF {paper_id} exists. Skipping.")
                # Treat as success but no bytes to save
                paper_info["pdf_saved_path"] = str(target_file)
                # await extracted_queue.put(paper_info) # Optional: Log as extracted?
                # For now, just skip
            else:
                # RUN THE DOWNLOAD
                pdf_bytes, result_meta = await download_one_paper(paper_info)
                
                if pdf_bytes:
                    logger.info({
                        "Paper_Id": paper_info.get('paper_id'),
                        "url_1": paper_info.get('url_1', ""),
                        "url_2": paper_info.get('url_2', ""),
                        "PDF_Extracted": True,
                        "Error": "No Error"
                    })
                    result_meta["pdf_bytes"] = pdf_bytes # Pass bytes to writer
                    await extracted_queue.put(result_meta)
                else:
                    await unextracted_queue.put(result_meta)

        except Exception as e:
            print(f"[Worker {worker_id}] Critical Error: {e}")
            paper_info["error"] = str(e)
            await unextracted_queue.put(paper_info)
        
        finally:
            task_queue.task_done()


async def writer(pdf_dir:Path, queue: asyncio.Queue, meta_path: Path, save_pdfs: bool = False):
    """
    Writes results to JSONL files and saves PDF bytes to disk.
    """
    # Ensure directory exists
    meta_path.parent.mkdir(parents=True, exist_ok=True)
    if save_pdfs and not pdf_dir.exists():
        pdf_dir.mkdir(parents=True, exist_ok=True)

    while True:
        item = await queue.get()
        if item is None:
            queue.task_done()
            return

        try:
            # Save PDF Bytes if present
            if save_pdfs and "pdf_bytes" in item:
                pdf_bytes = item.pop("pdf_bytes") # Remove bytes from metadata
                paper_id = item.get("paper_id")
                
                target_file = pdf_dir / f"{paper_id}.pdf"
                # Write sync (fast enough for SSD) or use run_in_executor
                target_file.write_bytes(pdf_bytes)
                logger.info(f"PDF Saved for {paper_id}")
                
                item["pdf_path"] = str(target_file)

            # Write Metadata
            async with aiofiles.open(meta_path, "a", encoding="utf-8") as f:
                await f.write(json.dumps(item) + "\n")

        except Exception as e:
            print(f"[Writer] Error: {e}")
        
        finally:
            queue.task_done()


async def main():
    # 1. Setup Queues
    task_queue = asyncio.Queue()
    extracted_queue = asyncio.Queue()
    unextracted_queue = asyncio.Queue()

    ap = parse_args()
    num_workers = ap.num_workers
    main_dir = ap.dir
    input_file = ap.paper_meta_file
    pdf_dir = ap.pdf_save_dir
    extracted_paper_meta_path = main_dir / f"extracted_paper_meta_{date.today():%Y-%m-%d}.json"
    unextracted_paper_meta_path = main_dir / f"unextracted_paper_meta_{date.today():%Y-%m-%d}.json"
    ap.log_dir.mkdir(parents=True, exist_ok=True)
    log_file_path = ap.log_dir / f"pdf_extraction_{date.today():%Y-%m-%d}.log"
    
    logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file_path),
        logging.StreamHandler() 
    ]
            )
    logger.info(f"Starting to process papers from {input_file}")



    # 2. Start Loader
    logger.info("[System] Loading papers...")
    loader_task = asyncio.create_task(load_papers_from_jsonl(input_file, task_queue))
    
    # Wait for loader to finish (simplifies poison pill logic)
    await loader_task

    # 3. Add Poison Pills for Workers
    for _ in range(num_workers):
        await task_queue.put(None)

    # 4. Start Workers
    logger.info(f"[System] Starting {num_workers} workers...")
    workers = [
        asyncio.create_task(worker_pdf_downloader(pdf_dir, task_queue, extracted_queue, unextracted_queue, i))
        for i in range(num_workers)
    ]

    # 5. Start Writers
    logger.info("[System] Starting writers...")
    extract_writer = asyncio.create_task(writer(pdf_dir,extracted_queue, extracted_paper_meta_path, save_pdfs=True))
    unextract_writer = asyncio.create_task(writer(pdf_dir,unextracted_queue, unextracted_paper_meta_path, save_pdfs=False))

    # 6. Wait for Workers to finish
    await asyncio.gather(*workers)
    logger.info("[System] All workers finished.")

    # 7. Signal Writers to finish
    await extracted_queue.put(None)
    await unextracted_queue.put(None)
    
    await asyncio.gather(extract_writer, unextract_writer)
    logger.info("[System] Extraction pipeline complete.")

if __name__ == "__main__":
    # if not pdf_dir.exists():
    #     pdf_dir.mkdir(parents=True, exist_ok=True)
    asyncio.run(main())
    
