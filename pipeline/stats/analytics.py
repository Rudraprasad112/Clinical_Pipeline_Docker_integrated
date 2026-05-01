# read data form ware house and applye transformation
import pandas as pd
import json
from datetime import datetime
import os
import matplotlib.pyplot as plt
from collections import Counter


# read 
# df = pd.read_parquet("D:/clovertex_assignments/datalake/refined/final_filter_genomics")
def task3a(df,file_name):

    # task 3A
    # age distribution 
    df['age'] = 2026 - pd.to_datetime(df['date_of_birth']).dt.year
    # distribution age
    df['age_group'] = pd.cut(
        df['age'],
        bins=[0,18,25,35,45,60],
        labels=['teenager',"adult","mature","responsible","Older"]
    )

    # store patient_task3a as parquet file 
    path = 'datalake/consumption/'
    try:
        df.to_parquet(f'{path}{file_name}',index=False)
        print(f"stored file{path}")
    except Exception as e:
        print(e)
    print('all done')

def task3b(df):
    
    lab_df = df.copy()
    stats = (
        lab_df.groupby('test_name')['test_value'].agg(mean='mean',median='median',std='std').reset_index()
    )
    # merge referance
    
    with open("data/reference/lab_test_ranges.json") as f:
        ref = json.load(f)

    rows = []


    for test, values in ref.items():
        rows.append({
            "test_name": test,
            "min": values.get("normal_low"),
            "max": values.get("normal_high"),
            'critical_low':values.get('critical_low'),
            'critical_high':values.get('critical_high')
        })

    ref_df = pd.DataFrame(rows)

    merg_df = lab_df.merge(ref_df,on="test_name",how='left')
    
    

    merg_df = merg_df[
        (merg_df['test_value'] < merg_df['min']) |
        (merg_df['test_value'] > merg_df['max'])
    ]

    # print(merg_df.head())

    merg_df = merg_df[
        merg_df["test_name"].isin(["hba1c", "creatinine"])
    ]
    
    
    merg_df = merg_df.sort_values(["patient_id", "test_name", "collection_date"])

    trend_df = merg_df.groupby(["patient_id", "test_name"]).agg(
        first_value=("test_value", "first"),
        last_value=("test_value", "last")
    ).reset_index()
    
    def get_trend(row):
        if row["last_value"] > row["first_value"]:
            return "worsening"
        elif row["last_value"] < row["first_value"]:
            return "improving"
        else:
            return "stable"

    trend_df["trend"] = trend_df.apply(get_trend, axis=1)

    
    # # store  as  aslab_statistics.parquet in analyst file
    path = 'datalake/consumption/'
    try:
        trend_df.to_parquet(f'{path}lab_statistics.parquet',index=False)
        print(f"stored file{path}")
    except Exception as e:
        print(e)
    print('all done')

def taks3c(df):
    
    diag_df = df.copy()
    ref_table = pd.read_csv("data/reference/icd10_chapters.csv")
    #Top 15 ICD-10 chapters by patient count (not diagnosis count)
    # dag_unique = diag_df[['patient_id',"icd10_code"]].drop_duplicates()

    # result = dag_unique.groupby('icd10_code')['patient_id'].nunique().reset_index()
    # result = result.rename(columns={"patient_id": "patient_count"})
    # top15 = result.sort_values("patient_count", ascending=False).head(15).reset_index()
    # top15 = top15[['icd10_code','patient_count']]

    # map code
    map_df = df.copy()
    # convert each ICD code and it's chapter
    ref_table[["start", "end"]] = ref_table["code_range"].str.split("-", expand=True)

    # find first 3 char of df icd10_code for finding range 
    map_df["code3"] = map_df["icd10_code"].str[:3]

    def map_chapter(code):
        for _, row in ref_table.iterrows():
            if row["start"] <= code <= row["end"]:
                return row["chapter_name"]   # or chapter name column
        return None

    map_df["chapter"] = map_df["code3"].apply(map_chapter)

    # top 15 chapters
    chep_result = map_df.groupby(['icd10_code','chapter'])['chapter'].nunique().reset_index(name='count')
    chep_result = chep_result.rename(columns={"count": "patient_count"})
    top15 = chep_result[['chapter','patient_count']]

    # save file
    
    filename = "diagnosis_frequency.parquet"
    path = 'datalake/consumption/'
    try:
        top15.to_parquet(f'{path}{filename}',index=False)
        print(f"stored file{path}")
    except Exception as e:
        print(e)
    print('all done')

def taske_d(df):
    
    genomics_df = df.copy()
    # task1
    #
    # find relaible records 
    df = df[genomics_df["read_depth"] >= 30] # less then 30 not relaible less experiment

    genomics_df = genomics_df[
        genomics_df["clinical_significance"].isin(
            ["Pathogenic", "Likely Pathogenic"]
        )
    ]
    result = genomics_df.groupby("gene").size().reset_index(name='count')
    # print(result)
    # top 5 gene use 
    top_5 = result.sort_values("count", ascending=False).head(5).reset_index()
    top_5 = top_5[['gene' , 'count']]
   
    # For each gene: mean allele frequency, 25th percentile, 75th percentile

    freq_df = df.copy()
    
    freq_df = freq_df.groupby(['gene','patient_id'])['allele_frequency'].agg(
        mean='mean',
        q25=lambda x:x.quantile(0.25),
        q75=lambda x:x.quantile(0.75)
    ).reset_index()

    

    # store file
    file_name = "variant_hotspots.parquet"
    path = 'datalake/consumption/'
    try:
        freq_df.to_parquet(f'{path}{file_name}',index=False)
        print(f"stored file{path}")
    except Exception as e:
        print(e)
    print('all done')

def task_3e(genomics_df,lab_df):
    lab_df = lab_df.drop_duplicates()
    genomics_df = genomics_df.drop_duplicates()

    hba1c_patients = lab_df[
        (lab_df["test_name"] == "hba1c") &
        (lab_df["test_value"] > 7.0)
    ]['patient_id'].unique()
    
    # 2. Genomics condition
    genetic_risk = genomics_df[
        genomics_df["clinical_significance"].isin(
            ["Pathogenic", "Likely Pathogenic"]
        )
    ]['patient_id'].unique()

    # 3. AND condition (intersection)
    high_risk_ids = set(hba1c_patients).intersection(set(genetic_risk))

    
    # 4. Convert to dataframe
    high_risk_df = lab_df[
        lab_df["patient_id"].isin(high_risk_ids)
    ]

    # merge for some extra info
    # print(high_risk_df)


    file_name = "high_risk_patients.parquet"
    path = 'datalake/consumption/'
    try:
        high_risk_df.to_parquet(f'{path}{file_name}',index=False)
        print(f"stored file{path}")
    except Exception as e:
        print(e)
    print('all done')

def task_3f():
    
    # print(patients_df)

    lab_dir = "datalake/refined/lab.parquet"
    diag_dir = "datalake/refined/diag.parquet"
    genomics_dir = "datalake/refined/genomics.parquet"
    patients_dir = "datalake/refined/patient.parquet"
    med_dir = "datalake/refined/med.parquet"

    lab_df = pd.read_parquet(lab_dir)
    diag_df = pd.read_parquet(diag_dir)
    genomics_df = pd.read_parquet(genomics_dir)
    patients_df = pd.read_parquet(patients_dir)
    med_df = pd.read_parquet(med_dir)
    # print("labs")
    # print(lab_df.head())


    patients_df["age_flag"] = (
        patients_df["admission_dt"] - patients_df["date_of_birth"]
    ).dt.days / 365

    # print('_______che_______________')
    # print(lab_df.head())

    lab_df["lab_flag"] = (
        (lab_df["test_value"] < 20.0) 
    )
    # print("_____dag_df__________")
    # print(diag_df)

    # find male + pragency
    diag_merge = diag_df.merge(patients_df, on="patient_id", how="left")

    diag_merge["gender_flag"] = (
        (diag_merge["sex"] == "M") &
        (diag_merge["icd10_code"].str.startswith("O"))
    )

    # Lab anomaly
    
    

    # Genomics anomaly
    genomics_df["geno_flag"] = (
        (genomics_df["allele_frequency"] > 1) |
        (genomics_df["allele_frequency"] < 0) |
        (genomics_df["read_depth"] < 10)
    )

    #medication anomaly (same time multiple meds)
    # print('droug')
    # print(med_df.head())
    med_df["med_flag"] = med_df.duplicated(
        subset=["patient_id", "dosage", "start_date"], keep=False
    )

    # Combine all anomalies
    # print(patients_df)
    # print(lab_df)
    # print(diag_df)
    # print(genomics_df)
    # print(med_df)
    age_ids = patients_df[patients_df["age_flag"] > 18]["patient_id"]
    lab_ids = lab_df[lab_df["lab_flag"]]["patient_id"]
    geno_ids = genomics_df[genomics_df["geno_flag"]]["patient_id"]
    med_ids = med_df[med_df["med_flag"]]["patient_id"]

    all_anomaly_ids = set(age_ids) | set(lab_ids)  | set(geno_ids) | set(med_ids)

    all_flags = list(age_ids) + list(lab_ids)  + list(geno_ids) + list(med_ids)

    count_dict = Counter(all_flags)

    anomaly_df = pd.DataFrame({
        "patient_id": list(count_dict.keys()),
        "anomaly_count": list(count_dict.values())
    })

    # store file
    file_name = "anomaly_flags.parquet"
    path = 'datalake/consumption/'
    try:
        anomaly_df.to_parquet(f'{path}{file_name}',index=False)
        print(f"stored file{path}")
    except Exception as e:
        print(e)
    print('all done')



def main():

    df = pd.read_parquet("datalake/refined/filter.parquet")
    task3a(df,"patient_summary.parquet")

    # task 3b
    lab_result_df = pd.read_parquet("datalake/refined/lab.parquet")

    task3b(lab_result_df)
    # # task3c
    diagnosis_df = pd.read_parquet("datalake/refined/diag.parquet")
    taks3c(diagnosis_df)

    # task 3d
    # 1.  Top 5 genes by count of Pathogenic or Likely Pathogenic variants (after filtering unreliable calls)
    #  2.  For each gene: mean allele frequency, 25th percentile, 75th percentile
    genomics_df = pd.read_parquet("datalake/refined/genomics.parquet")
    taske_d(genomics_df)

    # # task 3e
    # "D:\clovertex_assignments\datalake\refined\final_filter_genomics"


    task_3e(genomics_df,lab_result_df)

    patients_df = pd.read_parquet("datalake/refined/patient.parquet")
    medication_df = pd.read_parquet("datalake/refined/med.parquet")

    task_3f()

    # drow polts


if __name__ == '__main__':
    main()