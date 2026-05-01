from pipeline.ingestion import ingest
import pandas as pd
from dateutil import parser


alpha,beta,lab,diag,med,genomics,clinical_notes = ingest.all_files()

def fix_date(x):
    try:
        return parser.parse(str(x), dayfirst=True)
    except:
        try:
            return parser.parse(str(x), dayfirst=False)
        except:
            return pd.NaT


def fix_contacts(cont):
    if not isinstance(cont, dict):
        return {'phone': 'Missing', 'email': 'Missing'}
    return {
        'phone': cont.get('phone', 'Missing'),
        'email': cont.get('email', 'Missing')
    }
def alpha_clean(alpha_df):
    #

    # ['patient_id', 'first_name', 'last_name', 'date_of_birth', 'sex', 'blood_group', 'admission_dt', 'discharge_dt', 'contact_phone', 'contact_email', 'site']
    # fix columns lastname date_of_birth,sex,blood_group,admission_dt,discharge_dt,contact_phone,contact_email

    alpha_df['last_name'] = alpha_df['last_name'].str.strip() # some extra spaces in last name left side
    

    alpha_df['date_of_birth'] = alpha_df['date_of_birth'].apply(fix_date)

    # sex column
    # print(alpha_df['sex'].unique())
    # if some columns are null then 
    alpha_df['sex'] = alpha_df['sex'].fillna(alpha_df['sex'].mode()[0]) 

    # blood group column
    # blood_group column having null values replce most common value
    #print(alpha_df['blood_group'].mode())
    alpha_df['blood_group']= alpha_df['blood_group'].fillna(alpha_df['blood_group'].mode()[0])

    #print(alpha_df['blood_group'].unique())

    # admision date change type 
    alpha_df['admission_dt'] = alpha_df['admission_dt'].apply(fix_date)
    # discharge_date change type
    alpha_df['discharge_dt'] = alpha_df['discharge_dt'].apply(fix_date)

    # contact_phone and contact_email having null values fill it missing 
    alpha_df['contact_phone'] = alpha_df['contact_phone'].fillna("Missing")

    alpha_df['contact_email'] = alpha_df['contact_email'].fillna("Missing")


    # print(alpha_df.isnull().sum())

    # drop duplicate records

    alpha_df.drop_duplicates(keep='first',inplace=True)

    #print(alpha_df[alpha_df.duplicated()])

    return alpha_df

def beta_clean(beta_df):

    
    # convert anytype of string format to datetime format
    beta_df['birthDate'] = beta_df["birthDate"].apply(fix_date)
    #print(beta_df['birthDate'].dtypes)

    # gender
    # print(beta_df['gender'].unique())
    # fill null values
    beta_df['gender'] = beta_df['gender'].fillna("not mentoned")
    beta_df['gender'] = beta_df['gender'].str.lower()
    # replace values
    beta_df['gender'] = beta_df['gender'].replace({
        'f':'female',
        'm':"male",
        '': beta_df['gender'].mode()[0],
    })
    
    # print(beta_df['gender'].unique())

    #
    # some value are null replace to most frequent value
    beta_df['bloodType'] = beta_df['bloodType'].fillna(beta_df['bloodType'].mode()[0])
    

    # check every object have a both phone and email attribute

    beta_df['contact'] = beta_df['contact'].apply(fix_contacts)

    # beta_df = beta_df.drop_duplicates() first convert dct to df then drop duplicates in pandas
    return beta_df

def lab_clean(lab_df):
    
    # 'lab_result_id', 'patient_ref', 'test_name', 'test_value', 'test_unit',
    #    'collection_date', 'ordering_physician', 'site_name'
    # rename patient_ref to patient_id

    lab_df = lab_df.rename(columns={
        'patient_ref':"patient_id"
    }) # use usefull records
    lab_df =lab_df.dropna(subset='test_value')

    lab_df['collection_date'] = lab_df['collection_date'].apply(fix_date)

    # drop duplicate records
    lab_df = lab_df.drop_duplicates()
    return lab_df

def diag_clean(diag_df):
    
    # 'diagnosis_id', 'patient_id', 'icd10_code', 'description',
    #    'diagnosis_date', 'diagnosing_physician', 'is_primary', 'notes',
    #    'severity', 'status'
    
    # drop note column all values are null
    diag_df = diag_df.drop('notes',axis=1)

    # diagnosis_date column having lot's of messy type date's format to convert one type of data
    diag_df['diagnosis_date'] = diag_df['diagnosis_date'].apply(fix_date)

    # mild having some null values fill to most frequent serverity  value
    diag_df['severity'] = diag_df['severity'].fillna(diag_df['severity'].mode()[0])
    
    diag_df = diag_df.drop_duplicates()
    return diag_df


def med_clean(med_df):
    
    # 'medication_id', 'patient_id', 'medication_name', 'dosage', 'route',
    #    'frequency', 'start_date', 'end_date', 'prescribing_physician',
    #    'status' 
    # end date having 215 null values means start but not ended
    med_df['end_date'] = med_df['end_date'].fillna('0-0-0')
    med_df['end_date'] = med_df['end_date'].apply(fix_date)
    med_df['start_date'] = med_df['start_date'].apply(fix_date) 

    med_df = med_df.drop_duplicates()
    return med_df



def genomics_clean(genomics_df):
    # 'variant_id', 'patient_ref', 'gene', 'chromosome', 'position',
    #    'ref_allele', 'alt_allele', 'variant_type', 'allele_frequency',
    #    'read_depth', 'clinical_significance', 'sample_date',
    #    'sequencing_platform'
    # patient_ref rename to patient_id
    genomics_df = genomics_df.rename(columns={
        'patient_ref':'patient_id'
    })
    # print(genomics_df.isnull().sum())
    # lower case all letters 
    genomics_df['gene'] = genomics_df['gene'].str.lower()
    
    genomics_df['clinical_significance'] = genomics_df['clinical_significance'].fillna("not mentoined")

    # sample_date
    genomics_df['sample_date'] = genomics_df["sample_date"].apply(fix_date)
    return genomics_df

def store_all_datasets(dfs):
    names = ["alpha","beta","lab","diag","med","genomics",'clinical_df']
    for item,name in zip(dfs,names):
        item.to_parquet(f"datalake/refined/{name}.parquet",index=False)
        print(f"stored file{item}.parquet")
    
def  clinical_notes_clean(clinical_notes):
    clinical_notes['note_date'] = clinical_notes['note_date'].apply(fix_date)
    return clinical_notes
def clean_dataframes(alpha,beta,lab,diag,med,genomics,clinical_notes):


    alpha = alpha_clean(alpha)
    # print(alpha.head())
    beta = beta_clean(beta)
    # print(beta.head())
    lab = lab_clean(lab)
    diag = diag_clean(diag)
    # print(diag.head())
    med = med_clean(med)
    # print(med.head())
    genomics = genomics_clean(genomics)
    # print(genomics.head())
    clinical_notes_df = clinical_notes_clean(clinical_notes)

    store_all_datasets([alpha,beta,lab,diag,med,genomics,clinical_notes_df])

def main():
    clean_dataframes(alpha,beta,lab,diag,med,genomics,clinical_notes)

if __name__ == "__main__":
    main()
