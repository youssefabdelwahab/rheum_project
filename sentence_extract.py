import json
import re
import spacy

nlp =  spacy.load("en_core_web_sm")

from pathlib import Path
import pandas as pd

def split_sentences(text):
    doc = nlp(text)
    return[sent.text.strip() for sent in doc.sents]

def extract_trial_ids(text: str):
    """
    match the most common patterns of the clinical trial identfiers 
    returns both the trial IDs and the sentences that contain them
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
        r'\bIRCT/\d{4}/\d{2}/\d{2}/\d+\b',
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

    combined_pattern = re.compile('|'.join(patterns))

    trial_ids = list(set(re.findall(combined_pattern, text)))

    sentences = split_sentences(text)

    matched_sentences = [
        sentence.strip()
        for sentence in sentences
        if re.search(combined_pattern, sentence)
    ]

    return {
        'trial_ids': trial_ids,
        'matched_sentences' : matched_sentences
    }

def extract_accepted_dates(text : str):
    """
    Extract 'Accepted for publication' date from JSONL record and write to CSV
    Priority:
    1. Accepted
    2. Accessed
    3. Generic Month Year
    """
    # Priority 1: Accepted
    accepted_pattern = re.compile(
        r"(?:Accepted(?: for publication)?)\s*[:,-]?\s*(\w+\s+\d{1,2},\s+\d{4})",
        re.IGNORECASE
    )
    match = accepted_pattern.search(text)
    if match:
        return match.group(1)

    # Priority 2: Accessed
    accessed_pattern = re.compile(
        r"(?:Accessed(?: on| Date)?)\s*[:,-]?\s*(\w+\s+\d{1,2},\s+\d{4})",
        re.IGNORECASE
    )
    match = accessed_pattern.search(text)
    if match:
        return match.group(1)

    # Priority 3: Generic Month Year (e.g., February 2020)
    month_year_pattern = re.compile(
        r"\b(?:January|February|March|April|May|June|July|August|September|October|November|December|"
        r"Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)[a-z]*\s+\d{4}\b",
        re.IGNORECASE
    )
    match = month_year_pattern.search(text)
    if match:
        return match.group()

    return None

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
                    trial_info = extract_trial_ids(data['paper_text'])
                    accepted_dates = extract_accepted_dates(data['paper_text'])
                    results.append({
                        'id' : data['id'],
                        'trial_id' : trial_info['trial_ids'],
                        'matched_sentences' : trial_info['matched_sentences'],
                        'accepted_dates' : accepted_dates 
                    })
                else:
                    print(f"'paper_text' key missing at line {line_number}")
            except json.JSONDecodeError as e:
                print(f"Error parsing line {line_number}: {e}")
    
    return pd.DataFrame(results)

if __name__ == "__main__":
    df_trials = extract_json_paper_text('database_testing\extracted_paper_info_thread.jsonl')
    print(df_trials.head())