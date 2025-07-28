import json
import pandas as pd

json_file = 'database_testing/extracted_paper_info_thread.jsonl'

ids = []
paper_url = []
doi = []

with open(json_file, 'r', encoding = 'utf-8') as f:
    for line in f:
        data = json.loads(line)
        ids.append(data.get('id'))
        paper_url.append(data.get('crossref_paper_link'))
        doi.append(data.get('doi'))

df = pd.DataFrame({
    'id' : ids,
    'paper_url' : paper_url,
    'doi' : doi
})

print(df.head())