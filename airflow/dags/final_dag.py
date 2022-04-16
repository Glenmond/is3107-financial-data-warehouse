from tabnanny import check
from airflow.decorators import dag
from airflow.utils.task_group import TaskGroup
import pendulum

from extract import extract_data_task_group
from write import write_to_gcs_task_group
from staging import gcs_to_staging_task_group
from transformation import transform_task_group
from load import load_dwh_task_group

from check_gcs_conn import check_gcs_conn
from email_alert import email_operator
from create_postgres_staging import create_pg_staging
from write_to_postgres import write_to_postgres

from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.utils.edgemodifier import Label

def _get_gcs_conn(**kwargs):
    available = kwargs['ti'].xcom_pull(task_ids=f"check_conn")
    if available:
        return "gcs_available"
    else:
        return "on_premise"
@dag(
    schedule_interval='@daily',
    description='Daily Extraction to GCS',
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False
)
def final_dag():
    start = DummyOperator(task_id="start")
    
    with TaskGroup("check_gcs_conn", prefix_group_id=False) as check_conn:
        check_gcs_conn()
    
    with TaskGroup("extract", prefix_group_id=False) as section_1:
        extract_data_task_group()

    with TaskGroup("write_to_gcs", prefix_group_id=False) as section_2a:
        write_to_gcs_task_group()

    with TaskGroup("gcs_to_staging", prefix_group_id=False) as section_3a:
        gcs_to_staging_task_group()

    with TaskGroup("transformation", prefix_group_id=False) as section_4a:
        transform_task_group()

    with TaskGroup("load_dwh", prefix_group_id=False) as section_5a:
        load_dwh_task_group()

    with TaskGroup("send_email", prefix_group_id=False) as email:
        email_operator()

    with TaskGroup("create_psql_staging", prefix_group_id=False) as section_1b:
        create_pg_staging()

    with TaskGroup("write_to_psql", prefix_group_id=False) as section_2b:
        write_to_postgres()

    end = DummyOperator(task_id='end', trigger_rule="none_failed_min_one_success")
    
    gcs_available = DummyOperator(task_id='gcs_available')

    on_premise = DummyOperator(task_id='on_premise')

    check_availability = BranchPythonOperator(task_id="check_gcs_avail", python_callable=_get_gcs_conn)

    start >> check_conn >> section_1 >> check_availability
    check_availability >> Label("Available!") >> gcs_available >> section_2a >> section_3a >> section_4a >> section_5a >> end
    check_availability >> Label("Errors found") >> on_premise >> email >> section_1b >> section_2b >> end
    
final = final_dag()