import pandas as pd
import json

def load_json_lines(file_path):
    """
    Reads a JSONL file and returns a list of dictionaries.
    """
    with open(file_path, 'r') as file:
        return [json.loads(line) for line in file]

def parse_specification_list(specifications_list):
    """
    Converts a list of 'Key: Value' strings into a dictionary.
    """
    parsed_data = {}
    for entry in specifications_list:
        if ":" in entry:
            key, value = entry.split(":", 1)
            parsed_data[key.strip()] = value.strip()
    return parsed_data

def extract_watch_metadata(row):
    """
    Normalizes watch data from either the 'data' or 'specifications' field.
    """
    # Check which field contains the technical specs
    raw_specs = row.get("specifications") or row.get("data") or []
    
    if isinstance(raw_specs, list):
        return parse_specification_list(raw_specs)
    
    return {}

def transform_to_clean_dataframe(raw_records):
    """
    Orchestrates the transformation of raw JSON objects into a structured DataFrame.
    """
    normalized_data = []
    
    for record in raw_records:
        specs = extract_watch_metadata(record)
        # Preserve the URL for reference
        specs["url"] = record.get("url")
        normalized_data.append(specs)
        
    return pd.DataFrame(normalized_data)

def export_watch_data(file_input_path, file_output_path):
    """
    Main execution flow to convert JSONL to a CSV table.
    """
    raw_records = load_json_lines(file_input_path)
    clean_dataframe = transform_to_clean_dataframe(raw_records)
    
    # Drop duplicates based on Reference number if it exists
    if "Reference number" in clean_dataframe.columns:
        clean_dataframe = clean_dataframe.drop_duplicates(
            subset=["Reference number"], 
            keep="first"
        )
    
    clean_dataframe.to_csv(file_output_path, index=False)

if __name__ == "__main__":
    export_watch_data("scraped_watches.jsonl", "watches_table.csv")