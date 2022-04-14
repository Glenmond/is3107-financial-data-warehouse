import airflow 
from datetime import timedelta 
from airflow import DAG 
from datetime import datetime, timedelta 
from airflow.operators.email import EmailOperator
from params import alert_email_receipient
default_args={
    "start_date":datetime(2021,1,1)
}

def email_operator():

    send_GCS_failure_email = EmailOperator(
        task_id='send_GCS_failure_email',
        to=f'{alert_email_receipient}',
        subject='GCS Failure',
        html_content=f"""
        
        Date: {datetime.today()}
        Data Loading to GCS failed, running on-premise extraction and loading now.

        """ 
    )

