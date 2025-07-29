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


df_2 = pd.read_excel('database_testing\Living database of RA trials_Latest version to share_withCRSID_2025.xlsx', usecols = ['recordid.','abstract'])
print(df_2.head())

df['id'] = df['id'].astype(str)
df_2['recordid.'] = df_2['recordid.'].astype(str)

df_merged = df.merge(df_2, left_on = 'id', right_on = 'recordid.' , how = 'left')
df_merged = df_merged.drop(columns = ['recordid.'])

print(df_merged.head())

df_merged.to_csv('research_paper_database/csv_for_labeling.csv', index=False)