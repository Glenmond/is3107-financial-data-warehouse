from tabnanny import check
from airflow.decorators import dag
from airflow.utils.task_group import TaskGroup
import pendulum

from extract import extract_data_task_group
from write import write_to_gcs_task_group
from staging import gcs_to_staging_task_group
from transformation import transform_task_group
from load import load_dwh_task_group
from create_postgres_staging import create_pg_staging
from write_to_postgres import write_to_postgres
from check_gcs_conn import check_gcs_conn
from write_from_postgres_to_gcs import write_from_postgres_to_gcs_task_group

from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator

def _get_gcs_conn(**kwargs):
    available = kwargs['ti'].xcom_pull(task_ids=f"check_conn")
    if available:
        return "gcs_available"
    else:
        return "end"
@dag(
    #schedule_interval='@daily',
    schedule_interval=None,
    description='Daily Extraction to GCS',
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False
)
def on_premise_dag():
    start = DummyOperator(task_id="start")
    
    with TaskGroup("check_gcs_conn", prefix_group_id=False) as check_conn:
        check_gcs_conn()

    with TaskGroup("load_from_pg_to_gcs", prefix_group_id=False) as pg_to_gcs:
        write_from_postgres_to_gcs_task_group()

    with TaskGroup("gcs_to_staging", prefix_group_id=False) as gcs_to_staging:
        gcs_to_staging_task_group()

    with TaskGroup("transformation", prefix_group_id=False) as transformation:
        transform_task_group()

    with TaskGroup("load_dwh", prefix_group_id=False) as load_dwh:
        load_dwh_task_group()

    end = DummyOperator(task_id='end', trigger_rule="none_failed_min_one_success")

    gcs_available = DummyOperator(task_id='gcs_available')
    
    check_availability = BranchPythonOperator(task_id="check_gcs_avail", python_callable=_get_gcs_conn)

    start  >> check_conn >> check_availability
    check_availability >> gcs_available >> pg_to_gcs >> gcs_to_staging >> transformation >> load_dwh >> end
    check_availability >> end
    
on_premise_dag = on_premise_dag()