import pandas as pd
import csv
import requests
import re
import pdfplumber
from urllib.parse import urlparse
import fitz
import os
import numpy as np
from tqdm import tqdm

input_csv = "unextracted_paper_info.csv"
output_csv = "updated_paper_info.csv"
url_column = "pdf_url"
content_type_column = "content_type"
regex_column = "extracted_info"
temp_pdf = "temp_downloaded.pdf"

def download_pdf(url, save_folder="downloaded_pdfs"):
    if not os.path.exists(save_folder):
        os.makedirs(save_folder)

    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()

        content_type = response.headers.get("Content-Type", "").lower()
        if "application/pdf" not in content_type:
            print(f"URL does not point to a PDF: {url}")
            return False
        
        filename = os.path.basement(url)
        if not filename.lower().endswith(".pdf"):
            filename += ".pdf"

        save_path = os.path.join(save_folder, filename)
        with open(save_path, "wb") as f:
            f.write(response.content)
        
        print(f"downloaded PDF: {save_path}")
        return True
    
    except Exception as e:
        print(f"Failed to download {url}: {e}")
        return False

def extract_trial_ids(text: str):
    """
    match the most common patterns of the clinical trial identfiers 
    """
    patterns = [
        #ClinicalTrials.gov
        r'\bNCT\d{6,8}\b', 
        #EU CT Register
        r'\bEUCTR\d{4}-\d{6}-\d{2}(?:-[A-Z]{2,3})?\b',
        r'\bEudraCT\s?\d{4}-\d{6}-\d{2}\b',
        #ISRCTN
        r'\bISRCTN\d{6,8}\b',
        #UMIN (Japan)
        r'\bUMIN\d{6,8}\b',
        #ChiCTR(China)
        r'\bChiCTR(?:-[A-Z]{2,3})?-\d{6,8}\b',
        #ACTRN(Australia/New Zealand)
        r'\bACTRN\d{14}\b',
        #JPRN(Japan)
        r'\bJPRN-[A-Z]+\d{6,8}\b',
        #Japic(Japan)
        r'\bJapicCTI-\d{6}\b',
        #CTRI(India)
        r'\bCTRI/\d{4}/\d{2}/\d{6}\b',
        #IRCT(Iran)
        r'\bIRCT\d{8,15}(?:[A-Z]\d+)?\b',
        r'\bIRCT/\d{4}/\d{2}/\d{2}/\d+\b'
        #DRKS(Germany)
        r'\bDRKS\d{6,8}\b',
        #NTR(Netherlands)
        r'\bNTR\d{4,8}\b',
        #PER(Peru)
        r'\bPER-\d{3,4}-\d{2}\b',
        #KCT(Korea)
        r'\bKCT\d{6,8}\b',
        #SLCTR(Sri Lanka),
        r'\bSLCTR/\d{4}/\d{3}\b',
        #ReBec(Brazil)
        r'\bRBR-[A-Za-z0-9]{6,10}\b',
        #PACTR(Pan African)
        r'\bPACTR\d{14,20}\b',
        #TCTR(Thailand)
        r'\bTCTR\d{13}\b',
        #CRiS(Korea Clinical Research Info Service)
        r'\bCRiS-KCT\d{7}\b',
        #LBCTR(Lebanan)
        r'\bLBCTR\d{8,12}\b',
        #Health Canada Clinical Trials database
        r'\bHC-CTD-\d{4}-\d{4}\b',
        #WHO Universal Trial Number
        r'\bU1111-\d{4}-\d{4}\b',
        #Ukraine - UCTR
        r'\bUCTR\d{11,15}\b',
        r'\bUCTR-\d{5,7}\b'
    ]

    trial_ids = []

    for pattern in patterns: 
        trial_ids += re.findall(pattern, text)
    
    print(trial_ids)
    return list(set(trial_ids))

def extract_accepted_dates(text : str):
    """
    Extract 'Accepted for publication' date from JSONL record and write to CSV
    """
    date_pattern = re.compile(r"Accepted for publication[\s,:-]*(\w+\s+\d{1,2},\s+\d{4})", re.IGNORECASE)
    match = date_pattern.search(text)
    return match.group(1) if match else None
    
def extract_text_from_pdf(pdf_path: str):
    try:
        doc = fitz.open(pdf_path)
        return "\n".join(page.get_text() for page in doc)
    except Exception as e:
        print(f"Error reading PDF: {e}")
        return ""
    
def analyze_url(url: str):
    if pd.isna(url) or not isinstance(url, str):
        return "unreachable", [], None
    
    try:
        response = requests.get(url, timeout=20, stream = True)
        content_type = response.headers.get("Content-Type", "").lower()

        if "application/pdf" in content_type or url.lower().endswith(".pdf"):
            pdf_bytes = response.content
            if not pdf_bytes:
                return "pdf", [], None
            with open(temp_pdf, "wb") as f:
                f.write(pdf_bytes)

            
            text = extract_text_from_pdf(temp_pdf)
            print(text)
            os.remove(temp_pdf)
            ids = extract_trial_ids(text)
            accepted_dates = extract_accepted_dates(text)
            return "pdf", ids, accepted_dates
        else:
            return "html", [], None
    except Exception as e:
        print(f"URL failed: {url} | {e}")
        return "unreachable", [], None
    
def process_csv(input_csv, url_column):
    df = pd.read_csv(input_csv)

    content_types = []
    extracted_infos = []
    accepted_dates = []

    for url in tqdm(df[url_column], desc= "Processing URLs"):
        ctype, ids, date = analyze_url(url)
        content_types.append(ctype)
        extracted_infos.append(", ". join(ids))
        accepted_dates.append(date)

    df[content_type_column] = content_types
    df[regex_column] = extracted_infos
    df["accepted_date"] = accepted_dates

    df.to_csv(output_csv, index = False)
    print(f"\n Updated {input_csv} with content type and extracted regex info")

if __name__ == "__main__":
    process_csv(input_csv, url_column)


