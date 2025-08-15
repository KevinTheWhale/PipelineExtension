Student Rating ETL Pipeline

A scalable Apache Airflow ETL pipeline that automates the ingestion, cleaning, and storage of unnamed University Student Opinion of Teaching Effectiveness survey data for downstream analytics and predictive modeling.

Overview

This project is designed to streamline the process of working with survey data by:
	1.	Ingesting daily batch CSV files parameterized by execution date.
	2.	Cleaning and preprocessing the dataset to remove duplicates, drop irrelevant columns, and handle missing values.
	3.	Loading the processed data into a database for long-term storage and future analysis.
	4.	Preparing for future integration of machine learning models for instructor rating predictions.

Pipeline Workflow
	•	Queue Data – Loads raw CSV dataset into the pipeline.
	•	Clean Data – Applies transformations including:
	•	Duplicate removal
	•	Semester-based filtering
	•	Dropping unnecessary columns
	•	Filtering out specific survey responses
	•	Recoding categorical values for organization
	•	Train Model (Placeholder) – Future step for logistic regression or other models.
	•	Save Results – Logs key metrics and persists output for auditing and analysis.

Tech Stack
	•	Workflow Orchestration: Apache Airflow (DAG & PythonOperator)
	•	Programming Language: Python (Pandas, NumPy)
	•	Database: TBD
	•	Version Control: Git & GitHub

Future Enhancements
	•	Use Airflow TaskFlow API for cleaner task dependencies and context handling.
	•	Replace large DataFrame XComs with path-based handoffs or staging tables.
	•	Add cloud storage integration (AWS S3, GCS) for intermediate artifacts.
	•	Expand machine learning step to include model training, evaluation, and reporting.
