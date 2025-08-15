from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago # for compact datetime
from datetime import datetime
import pandas as pd
import numpy as np
from pathlib import Path

def queue_data(ds=None,**kwargs): # Extract
    if(ds is None):
        ds = datetime.today().strftime('%Y-%m-%d')
        print(ds)
    print("Loading SOTE data from CSV...")
    # Batch Processing
    print(f"Execution date {ds}")
    file_path = f"/Users/Kevin/Documents/Developer/SOTE/SOTE/SOTE_BATCH/{ds}.csv" # Airflow gives this automatically (e.g., '2025-04-14')
    sote_df = pd.read_csv(file_path)
    print(sote_df.shape)
    print(sote_df.head())
    return sote_df # return the newly queued df

def clean_data(sote_df): # Transform
    print("Cleaning and preprocessing...")
    # Check for duplicates
    
    # Check by year:
    sote_yearOne = sote_df[sote_df[['Semester'].isin([2144,2152])]]
    sote_yearTwo = sote_df[sote_df[['Semester'].isin([2164,2172])]]

    clean_yearOne = sote_yearOne.drop_duplicates()
    clean_yearTwo = sote_yearTwo.drop_duplicates()

    # combine into one df
    sote_clean = pd.concat([clean_yearOne,clean_yearTwo],ignore_index=True)

    # Drop unwanted columns: 
    drop_cols = [sote_df[i] for i in [3,7,8,10,15]]
    sote_clean = sote_clean.drop(columns=drop_cols)

    # Filter out Q16 and Q17:
    sote_clean = sote_clean[sote_clean['Question.16'] != '2']
    sote_clean = sote_clean[sote_clean['Question.17'] != '2']

    sote_clean = sote_clean.drop(columns=['Question.16','Question.17'])

    # Handle missing values:
    na_count = sote_clean.isna().sum()
    sote_clean = sote_clean.drop(columns=[sote_clean.columns[8]]) # record number
    sote_clean = sote_clean.dropna()

    ######################################################################################################

    # Recode semester: (For organization)
    sote_clean['Semester'] = sote_clean['Semester'].astype('category')
    semester_map = {
        2144: 'Fall 14', 2152: 'Spring 15',
        2164: 'Fall 16', 2172: 'Spring 17'
    }
    sote_clean['Semester'] = sote_clean['Semester'].replace(semester_map)

    # Recode 



def train_model():
    print("Training logistic regression model...")

def save_results(): # Load
    print("Saving metrics to log...")

default_args = {
    'start_date': datetime(2024, 1, 1),
    'retries': 1
}
queue_data()


with DAG(
    dag_id='sote_model_pipeline',
    schedule_interval='@daily',
    default_args=default_args,
    catchup=False
) as dag:

    task1 = PythonOperator(
    task_id='queue_data',
    python_callable=queue_data,
    op_kwargs={'ds': '{{ ds }}'},)  # pass execution date string
    task2 = PythonOperator(task_id='clean_data', python_callable=clean_data)
    task3 = PythonOperator(task_id='train_model', python_callable=train_model)
    task4 = PythonOperator(task_id='save_results', python_callable=save_results)

    task1 >> task2 >> task3 >> task4  # Set execution order

