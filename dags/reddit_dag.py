# importing libarries
import os
import sys
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

# Adding the parent directory to the Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipelines.aws_s3_pipeline import upload_s3_pipeline
from pipelines.reddit_pipeline import reddit_pipeline

# Setting the default arguments for the DAG, such as the owner & stating dates
default_args = {
    'owner': 'Sumaiya Uddin',
    'start_date': datetime(2024, 9, 18)
}

# Generating a date string using datetime.now().strftime("%Y%m%d"). 
# This is used to append a timestamp to filenames to avoid overwriting files.
file_postfix = datetime.now().strftime("%Y%m%d")

dag = DAG(
    dag_id='etl_reddit_pipeline',      # unique identifier for the DAG.
    default_args=default_args, 
    schedule_interval='@daily',        # Specifies that this DAG should run once daily.
    catchup=False,                     # Ensures that Airflow does not backfill (run missed schedules) for the period between the start_date and the current date.
    tags=['reddit', 'etl', 'pipeline'] # categorizing and filtering the DAG in the Airflow UI.
)

# This DAG consists of two tasks below
# extraction from reddit
extract = PythonOperator(
    task_id='reddit_extraction',       # Unique identifier for the task.
    python_callable=reddit_pipeline,   # Calls the reddit_pipeline function to execute this task
    op_kwargs={                        # Passes keyword arguments to reddit_pipeline
        'file_name': f'reddit_{file_postfix}', # Sets the file name for storing the extracted Reddit data, with a timestamp.
        'subreddit': 'dataengineering', # Specifies the subreddit to extract data from (dataengineering).
        'time_filter': 'day',           # Filters posts by time (day), meaning posts from the last day.
        'limit': 100                    # Limits the number of posts to extract to 100.
    },
    dag=dag
)

# upload to s3
upload_s3 = PythonOperator(              # Defines a PythonOperator task named 's3_upload'
    task_id='s3_upload',                 # Unique identifier for the task.
    python_callable=upload_s3_pipeline,  # Calls the upload_s3_pipeline function to execute this task.
    dag=dag                              # Associates this task with the defined DAG.
)

extract >> upload_s3                     # Sets up a dependency between tasks using the >> operator.
                                         # This means that the upload_s3 task will start only after the extract task has successfully completed.