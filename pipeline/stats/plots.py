import matplotlib.pyplot as plt
import pandas as pd
import json
import os
import seaborn as sns

# mak dir 


path = "datalake/consumption/plots"

if not os.path.exists(path):
    os.makedirs(path)
    print("folder created")
else:
    print("already exists")

def patient_demo_graphy():
    # age distribution
    patients = pd.read_parquet("datalake/consumption/patient_summary.parquet")
    # print(patients.head())
    # print(os.path.exists("D:/clovertex_assignments/datalake/consumption/patient_summary.parquet"))

    plt.figure()
    sns.histplot(patients['age'],bins=6)
    plt.title("Age distribution")
    plt.xlabel("Age")
    plt.ylabel("count")

    try:
        plt.savefig("datalake/consumption/plots/age_distribution.png")
        print("ploted")
    except Exception as e:
        print("err___________:",e)

    # gender bar char
    plt.figure()
    patients['sex'].value_counts().plot(kind='bar')
    plt.title("Gender Distribution")
    plt.xlabel("Gender")
    plt.ylabel("Count")

    try:
        plt.savefig("datalake/consumption/plots/gender_distribution.png")
        print("ploted")
    except Exception as e:
        print(e)
    
def Diagnosis_frequancy():
    df = pd.read_parquet("datalake/consumption/diagnosis_frequency.parquet")

    
    top_15 = df.sort_values('patient_count',ascending=False).head(15).reset_index()
    chapter_count = top_15.groupby(top_15['chapter'])['patient_count'].agg(sum).reset_index(name='count')

    # plot barh chart
    plt.figure(figsize=(10,5))
    plt.barh(chapter_count["chapter"], chapter_count["count"])

    plt.xlabel("Patient Count")
    plt.ylabel("ICD-10 Chapter")
    plt.title("Diagnosis Frequency by Chapter")
    try:
        plt.savefig("datalake/consumption/plots/diagnosis_frequency.png")
        print("ploted")
    except Exception as e:
        print(e)



def Lab_result_distribution():
    df = pd.read_parquet("datalake/consumption/lab_statistics.parquet")
    print(df.head())

    plt.figure(figsize=(10,4))

# hba1c
    plt.subplot(1,2,1)
    hba1c = df[df["test_name"] == "hba1c"]["first_value"]
    plt.hist(hba1c)
    plt.axvline(4.0)
    plt.axvline(5.7)
    plt.title("hba1c")

    # creatinine
    plt.subplot(1,2,2)
    creatinine = df[df["test_name"] == "creatinine"]["first_value"]
    plt.hist(creatinine)
    plt.axvline(0.6)
    plt.axvline(1.3)
    plt.title("creatinine")

    plt.tight_layout()
    try:
        plt.savefig("datalake/consumption/plots/hba1c_distribution.png")
        print("ploted")
    except Exception as e:
        print(e)
    
def genomics_plot():
    df = pd.read_parquet("datalake/refined/genomics.parquet")

    print(df.head())

    plt.figure(figsize=(8,6))

    # get unique categories
    categories = df["clinical_significance"].unique()

    for cat in categories:
        subset = df[df["clinical_significance"] == cat]
        
        plt.scatter(
            subset["allele_frequency"],
            subset["read_depth"],
            label=cat
        )

    plt.xlabel("Allele Frequency")
    plt.ylabel("Read Depth")
    plt.title("Allele Frequency vs Read Depth")

    plt.legend()
    try:
        plt.savefig("datalake/consumption/plots/genomics_scatter.png")
        print("ploted")
    except Exception as e:
        print(e)
    

    # plt.show()
def high_risk_patients():
    df = pd.read_parquet("datalake/consumption/high_risk_patients.parquet")

    print(df.head())
    ref = {
        "creatinine": (0.6, 1.3),
        "glucose_fasting": (70, 100),
        "sodium": (135, 145),
        "hemoglobin": (12, 17),
        "alt": (7, 56)
    }

    def is_abnormal(row):
        if row["test_name"] in ref:
            low, high = ref[row["test_name"]]
            return row["test_value"] < low or row["test_value"] > high
        return False

    df["abnormal"] = df.apply(is_abnormal, axis=1)
    risk = df.groupby("patient_id")["abnormal"].sum()
    

    risk = risk.sort_values(ascending=False)

    plt.figure(figsize=(8,5))
    plt.bar(risk.index, risk.values)

    plt.xticks(rotation=45)
    plt.xlabel("Patient ID")
    plt.ylabel("Abnormal Test Count")
    plt.title("High-Risk Patient Summary")
    plt.tight_layout()
    try:
        plt.savefig("datalake/consumption/plots/High_risk_Summary.png")
        print("ploted")
    except Exception as e:
        print(e)
    
    
def anomaly():
    df = pd.read_parquet('datalake/consumption/anomaly_flags.parquet')
    patients_df = pd.read_parquet("datalake/refined/patient.parquet")
    total_patient = patients_df['patient_id'].value_counts().sum()
    total_anomaly = df['patient_id'].value_counts().sum()

    plt.figure(figsize=(10,5))
    plt.barh("Total Records", total_anomaly)
    plt.barh("Total Anomaly's", total_patient)
    plt.xlabel("Patient Count")
    plt.ylabel("ICD-10 Chapter")
    plt.title("Diagnosis Frequency by Chapter")
    try:
        plt.savefig("datalake/consumption/plots/data_quality_overview.png")
        print("ploted")
    except Exception as e:
        print(e)
    

def main():

    patient_demo_graphy()
    Diagnosis_frequancy()
    Lab_result_distribution()
    genomics_plot()
    high_risk_patients()
    anomaly()
if __name__ == '__main__':
    main()