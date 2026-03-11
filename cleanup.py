import pandas as pd
import json

def load_jsonl_to_dataframe(file_path):
    """
    Reads a JSONL file and returns a pandas DataFrame.
    """
    with open(file_path, 'r') as file:
        data = [json.loads(line) for line in file]
    return pd.DataFrame(data)

def clean_price(price_string):
    """
    Removes currency symbols and commas to convert price to a float.
    """
    if not isinstance(price_string, str):
        return price_string
    
    numeric_price = price_string.replace('$', '').replace(',', '').strip()
    return float(numeric_price)

def extract_reference_number(spec_list):
    """
    Parses the specifications list to find the Reference Number.
    """
    prefix = "Reference number: "
    for item in spec_list:
        if item.startswith(prefix):
            return item.replace(prefix, "").strip()
    return None

def process_watch_data(raw_dataframe):
    """
    Orchestrates the cleaning and transformation of the watch DataFrame.
    """
    df = raw_dataframe.copy()
    
    df['price_usd'] = df['Price'].apply(clean_price)
    df['reference_number'] = df['specifications'].apply(extract_reference_number)
    
    return df[['url', 'Brand', 'Model', 'reference_number', 'price_usd', 'Year of production']]

# Usage
raw_df = load_jsonl_to_dataframe('watches.jsonl')
clean_df = process_watch_data(raw_df)

with open("output.json","w") as f:
    json.dump(f, clean_df)