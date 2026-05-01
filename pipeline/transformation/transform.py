import json
from pipeline.cleaning import clean
import pandas as pd
from datetime import datetime
import os

# print(data_frames.beta.head())
# print(data_frames.alpha.head())

# alpha table  column alerady good
# beta table column haivng values in nested manner
# convert good format
# print(data_frames.alpha.columns)
# print(data_frames.beta.columns)
# alpha columns 'patient_id', 'first_name', 'last_name', 'date_of_birth', 'sex',
#        'blood_group', 'admission_dt', 'discharge_dt', 'contact_phone',
#        'contact_email', 'site'

# # beta table columns ['patientID', 'name', 'birthDate', 'gender', 'bloodType', 'encounter',
#        'contact'

# rename columns in beta table
def beta_split_to_df(beta_df):
    beta_df = beta_df.rename(columns={
        'patientID':"patient_id","birthDate":'date_of_birth','gender':'sex','bloodType':'blood_group'
    })

    # print(beta_df.columns)

    # split names
    beta_df["first_name"] = beta_df['name'].apply(lambda nm:nm.get('given')if isinstance(nm,dict)else None)
    beta_df['last_name'] = beta_df['name'].apply(lambda nm:nm.get('family')if isinstance(nm,dict)else None)

    # print(beta_df.columns)
    # split join related dates
    # print(beta_df['encounter'][0])

    beta_df['admission_dt'] = beta_df['encounter'].apply(lambda dt:dt.get('admissionDate')if isinstance(dt,dict)else None)
    beta_df['discharge_dt'] = beta_df['encounter'].apply(lambda dt:dt.get("dischargeDate")if isinstance(dt,dict)else None)
    beta_df['site'] = beta_df['encounter'].apply(lambda typ:typ.get("facility")if isinstance(typ,dict)else None)

    # split contact details
    beta_df['contact_email'] = beta_df['contact'].apply(lambda eml:eml.get("email")if isinstance(eml,dict)else None)
    beta_df['contact_phone'] = beta_df['contact'].apply(lambda cnm:cnm.get("phone")if isinstance(cnm,dict)else None)


    # print(beta_df['contact'][0])
    # print(beta_df['contact_email'])


    return beta_df[['patient_id', 'first_name', 'last_name', 'date_of_birth', 'sex',
        'blood_group', 'admission_dt', 'discharge_dt', 'contact_phone',
        'contact_email', 'site']]



def merge_dataframes(patient_df):
    # print(patient_df.dtypes)
    # rest tanles
    lab_dir = "datalake/refined/lab.parquet"
    diag_dir = "datalake/refined/diag.parquet"
    genomics_dir = "datalake/refined/genomics.parquet"
    patients_dir = "datalake/refined/patient.parquet"
    med_dir = "datalake/refined/med.parquet"


    lab_df = pd.read_parquet(lab_dir)
    diag_df = pd.read_parquet(diag_dir)
    genomics_df = pd.read_parquet(genomics_dir)
    med_df = pd.read_parquet(med_dir)
    # rename the columns

    genomics = genomics_df.rename(columns={
        'patient_ref':'patient_id'
    })
    lab_result = lab_df.rename(columns={
        'patient_ref':'patient_id'
    })
    # print(lab_result.head())
    # print(medications.head())
    # print(diagnoses.head())
    # print(genomics.head())
    # read all files
    
    # merge logic
    merged_df = patient_df \
        .merge(lab_df,how='left') \
        .merge(diag_df,how='left') \
        .merge(med_df,how='left') \
        .merge(genomics_df,how='left')
    return merged_df

def apply_genomics_filter(merg_df):
    final_df = merg_df
    filtered_df = final_df[ 
        (
            (final_df['read_depth'] >= 10) &
            (final_df['allele_frequency'] >= 0.01) &
            (final_df['clinical_significance'].isin(['Pathogenic', 'Likely pathogenic'])))]
    # print(filtered_df.isnull().sum())
    return filtered_df

def create_report(df,final_df):
    rep = {
        "dataset":"final_patient_data",
        "rows_in":len(final_df),
        "row_out":len(df),

        "issue_found":{
            "duplicates_removed": int(df.duplicated().sum()),
            'nulls_handled': int(df.isna().sum().sum()),
            'encoding_fixed': 0
        },
        "processing_timestamp": datetime.utcnow().isoformat()
    }
    # print(json.dumps(rep,indent= 4))
    with open("datalake/refined/data_quality_report.json", "w") as f:
        json.dump(rep, f, indent=4)

def store_file(df,path,file_name):
    df.to_parquet(f'{path}{file_name}',index=False)
    manifest_update(df,path,file_name)

def store_labresult_file(df,path,partition_col):
    os.makedirs(path, exist_ok=True)

    df.to_parquet(
        path,
        partition_cols=[partition_col],
        index=False
    )
    manifest_update(df,path,'lab_partitioned_results')

def manifest_update(df,path,file_name):
    new_entry = {
        "file_name" : file_name,
        'filepath' : path,
        'row_count' : len(df),
        'schema' : {col: str(dtype) for col, dtype in df.dtypes.items()},
        'processing_timestamp': datetime.utcnow().isoformat()
    }

    # if manifest exist read it
    manifest = {}
    if os.path.exists(f'{path}manifest.json'):
        with open(f'{path}manifest.json','r') as f:
            manifest = json.load(f)
    else:
        manifest = {"files": []}

    # check file exists or not
    file_exists = False

    for i,file in enumerate(manifest['files']):
        if file['file_name'] == file_name:
            manifest['files'][i] = new_entry
            file_exists = True
            break
    if not file_exists:
        manifest['files'].append(new_entry)
    
    with open(f'{path}manifest.json','w') as f:
        json.dump(manifest,f,indent=4)
    

def main():
    alpha_dir = "datalake/refined/alpha.parquet"
    beta_dir = "datalake/refined/beta.parquet"
    alpha_df = pd.read_parquet(alpha_dir)
    beta_df = pd.read_parquet(beta_dir)
    beta_df = beta_split_to_df(beta_df)

    # print(beta_df.head())
    # clean beta_modified dataframe
    clean_beta_df = clean.alpha_clean(beta_df)
    # merge alphe and beta dataframe
    # create a single patient table merging clean_beta_df and alpha_df
    alpha_df = clean.alpha
    # merge
    # print("____clean check__________")
    # print(alpha_df['sex'].unique())
    # print(clean_beta_df['sex'].unique())
    patient = pd.concat([alpha_df,clean_beta_df],ignore_index=True)

    # remove duplicates 
    patient = patient.drop_duplicates(subset='patient_id')
    
    # duplicate sex column
    patient['sex'] = patient['sex'].replace({
        'F':"female",
        "M":'male',
    })

    patient_cols = ['date_of_birth', 'admission_dt', 'discharge_dt']

    for col in patient_cols:
        patient[col] = pd.to_datetime(patient[col], errors='coerce')
        patient[col] = patient[col].astype('datetime64[ns]')
    
    store_file(patient,'datalake/refined/',"patient.parquet")
    # merge all df to patient df
    merged_df = merge_dataframes(patient)
    # apply filter genomics quality columns
    filtered = apply_genomics_filter(merged_df)
    # print("________________filtered__________________")

    #
    # print(filtered.columns)

    # store filtered data as a  parquet file
    store_file(filtered,'datalake/refined/',"filter.parquet")
    # create a programmatic report
    create_report(filtered,merged_df)
    # create a lab_result table and store in partition manner
    lab_cols = ['lab_result_id','test_name', 'test_value',
        'test_unit', 'collection_date', 'ordering_physician', 'site_name']
    
    lab_results = filtered[lab_cols].dropna(subset=['lab_result_id'])

    store_labresult_file(lab_results,"datalake/refined/lab_result/","collection_date")# data not store because all are non and remove non value's
    

if __name__ == "__main__":
    main()
    