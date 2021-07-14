import pandas as pd

def import_from_csv(csv_file):
    df = pd.read_csv(csv_file)
    return df



