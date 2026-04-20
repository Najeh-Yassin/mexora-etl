import pandas as pd
import json
import logging

def extract_csv(path):
    df = pd.read_csv(path, dtype=str)
    logging.info(f"[EXTRACT] {path} : {len(df)} lignes.")
    return df

def extract_json(path):
    with open(path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    df = pd.DataFrame(data['produits']).astype(str)
    logging.info(f"[EXTRACT] {path} : {len(df)} lignes.")
    return df