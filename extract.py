import pandas as pd
import csv

def extract():
    url="FakeNamesUK1.csv"
    data=pd.read_csv(url, low_memory=False)
    print(f"{len(data)} will be extracted and loaded and transformed")
    return data
