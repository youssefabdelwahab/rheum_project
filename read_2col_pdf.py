import fitz
import re
import os
import numpy as np
import json
import pandas as pd

def is_full_width(block, page_width, threshold=0.8):
    """
    Check if the block spans for the entire page width
    Like title, some abstracts, tables, graphs

    The bounding box (bbox) helps identify the layout structure, ignore headers and footers 
    reconstruct reading order and extract elements
    """
    x0, y0, x1, y1 = block["bbox"]
    width = x1 -x0
    return width >= page_width*threshold

def is_header_or_footer(block, page_height, margin = 25):
    """
    Identifies if the block is a header or the footer area
    The margin decides how much of the top and bottom has to be ignored
    """
    y0, y1 = block["bbox"][1], block["bbox"][3]
    return y1 < margin or y0 > (page_height - margin)

def is_table_like(block, digit_threshold=0.3, line_threshold=3):
    """
    Check if the content has high digit ratio or many numbers
    see if the content has many short lines that are aligned
    The block is likely a table
    """
    lines = block["lines"]
    
    if len(lines) < line_threshold:
        return False

    text_content = [span["text"] for line in lines for span in line["spans"] if "text" in span]
    all_text = " ".join(text_content)
    if not all_text:
        return False
    digit_ratio = sum(c.isdigit() for c in all_text) / len(all_text)
    return digit_ratio > digit_threshold

def block_text(block):
    """
    Convert a block of text with lines and spans into plain text
    """
    lines = block.get("lines",[])
    texts = []
    for line in lines:
        line_text = " ".join(span['text'] for span in line.get("spans", []))
        texts.append(line_text)
    return "\n".join(texts)

def extract_text_two_cols(pdf_path):
    """
    Read content from the URL 
    This case reads content from the PDFs
    """
    doc = fitz.open(pdf_path)
    full_text = ""

    """
    read the content returns as a dictionary version of the page number and the contents of the page organized as blocks
    type == 0 means text block only
    the line ignores images, drawings and tables 
    """
    for page_num, page in enumerate(doc):
        blocks = [
            b for b in page.get_text("dict")["blocks"] 
            if b["type"] == 0 
            and not is_header_or_footer(b, page.rect.height)
            and not is_table_like(b)
            ]
        #Getting the width of the page
        page_width = page.rect.width

        """
        separating the contents with full-width and two column blocks
        """
        col_blocks = []

        for b in blocks:
            if is_header_or_footer(b, page.rect.height):
                continue
            if is_table_like(b):
                continue
            col_blocks.append(b)
        
        left_col = []
        right_col = []

        for b in col_blocks:
            x0, y0, x1, y1 = b["bbox"]
            center_x = (x0 + x1) / 2
            if center_x < page.rect.width / 2:
                left_col.append(b)
            else:
                right_col.append(b)
        
        left_col.sort(key = lambda b: b["bbox"][1])
        right_col.sort(key = lambda b: b["bbox"][1])

        page_text = ""
        for b in left_col + right_col:
            page_text += block_text(b) + "\n\n"
    
        full_text += f"\n--- Page {page_num + 1} ---\n" +page_text        

    return full_text

def extract_json_paper_text(jsonl_file_path):
    """
    Extract the paper_text content from each line in a .jsonl file
    The text is then run through a function to obtain the clinical trial numbers 
    """
    results = []
    with open(jsonl_file_path, 'r', encoding = 'utf-8') as f:
        for line_number, line in enumerate(f, start=1):
            try:
                data = json.loads(line)
                if 'paper_text' in data and 'id' in data:
                    trial_id = extract_trial_ids(data['paper_text'])
                    results.append({
                        'id' : data['id'],
                        'trial_id' : trial_id
                    })
                else:
                    print(f"'paper_text' key missing at line {line_number}")
            except json.JSONDecodeError as e:
                print(f"Error parsing line {line_number}: {e}")
    
    return pd.DataFrame(results)

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
        r'EUCTR\d{4}-\d{6}-\d{2}',
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
        r'\bUCTR-\d{5,7}\b',
        #ISRCTN
        r"\bISRCTN\d{6,8}\b",
        #Others,
        r"CTRI/\d{4}/\d{2}/\d{6}",
        r"\b[A-Z]{2}\d{4}\b",
        r"\bDARWIN\s*\d+\b",
        r"\bNTR\d+\b",
        r"\bUMIN\d{9}\b"
    ]

    trial_ids = []

    for pattern in patterns: 
        trial_ids += re.findall(pattern, text)

    return list(set(trial_ids))

def extract_from_pdf_or_text(input_source: str):
    """
    Automatically detects whether the input is a path to the pdf file or plain text
    If input source is a valid PDF file(ends with .pdf), it reads and extracts text from it
    Otherwise, treats input_source as raw_text
    """
    if os.path.isfile(input_source) and input_source.lower().endswith('.pdf'):
        print("Detected Input PDF file:", input_source)
        text = extract_text_two_cols(pdf_path)
    else:
        print("Detected raw text input")
        text = input_source

    trial_ids = extract_trial_ids(text)
    return trial_ids


if __name__ == "__main__":
    pdf_folder= "downloaded_pdfs"
    pdf_files = [os.path.join(pdf_folder, f) for f in os.listdir(pdf_folder) if f.endswith('.pdf')]

    for pdf_path in pdf_files:
        trial_ids_pdf = extract_from_pdf_or_text(pdf_path)
        print("For PDF:", pdf_path)
        print("Extracted trial numbers:", trial_ids_pdf)


    df_trials = extract_json_paper_text('database_testing\extracted_paper_info_thread.jsonl')
    print(df_trials.head())


    columns_to_merge = ['recordid.', 'clinical_reg_no']
    df = pd.read_excel('database_testing\Living database of RA trials_Latest version to share_withCRSID_2025.xlsx', usecols = columns_to_merge)

    df_trials['id'] = df_trials['id'].astype(str)
    df['recordid.'] = df['recordid.'].astype(str)

    merged_df = pd.merge(df_trials, df, left_on = "id", right_on = "recordid.", how = "left")

    print(merged_df.head(20))

    def is_clinical_id_in_trial_id(row):
        clinical_str = row.get('clinical_reg_no')
        trial_ids = row.get('trial_id')

        if (pd.isna(clinical_str) or clinical_str == ''):
            clinical_ids = []
        else:
            clinical_ids = [x.strip() for x in re.split(r'[;,]', clinical_str)]

        if not clinical_ids and not trial_ids:
            return True
        
        if isinstance(trial_ids, list) and clinical_ids:
            return all(cid in trial_ids for cid in clinical_ids)   

    merged_df['clinical_id_match'] = merged_df.apply(is_clinical_id_in_trial_id, axis = 1 )
    print(merged_df.head(20))

    merged_df.to_csv("database_testing\match_output.csv", index = False)