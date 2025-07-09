import pandas as pd
import json

def filter_unmatched_json(jsonl_path, df_path, output_jsonl_path):
    """
    This function filters the JSONL record where the ID is marked as Unmatched (match == False)
    This function takes in 3 arguments for df file path, the main JSON file and the output file

    The dataframe is filtered to have rows where match == False,then selects the ID column and convert the datatype to string and add as a list

    infile - source JSONL file for reading
    outfile - new file where unmatched JSON record will be written

    for each line in the infile, function parse each line and gets the value associated with id key and gets the id as str
    if 'id' is missing, defaults to an empty string

    if the id match the list , writes the line in the outfile
    """
    df = pd.read_csv(df_path)
    unmatched_ids = df[df['clinical_id_match'] == False]['id'].astype(str).tolist()
    
    with open(jsonl_path, 'r', encoding='utf-8') as infile, open(output_jsonl_path, 'w', encoding='utf-8') as outfile:
        for line in infile:
            try:
                record = json.loads(line)
                record_id = str(record.get('id',''))

                if record_id in unmatched_ids:
                    outfile.write(json.dumps(record) + '\n')

            except json.JSONDecodeError:
                continue

    print(f"Filtered JSONL with unmatched records saved to: {output_jsonl_path}")


filter_unmatched_json(
    jsonl_path = 'database_testing/extracted_paper_info_thread.jsonl',
    df_path = 'database_testing/match_output.csv',
    output_jsonl_path = 'database_testing/unmatched_paper_info.jsonl'
)