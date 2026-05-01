import pandas as pd
import json
from datetime import datetime

def all_files():


    alpha = pd.read_csv("data/site_alpha_patients.csv")
    beta = pd.read_json("data/site_beta_patients.json")
    lab = pd.read_parquet("data/site_gamma_lab_results.parquet")
    diag = pd.read_csv("data/diagnoses_icd10.csv")
    med = pd.read_json("data/medications_log.json")
    genomics = pd.read_parquet("data/genomics_variants.parquet")
    clinical_notes = pd.read_csv("data/clinical_notes_metadata.csv")

    return alpha,beta,lab,diag,med,genomics,clinical_notes

def main():
    all_files()
# return all the files
if __name__ == '__main__':

    main()