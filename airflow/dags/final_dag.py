from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
import pendulum
from extract import extract_data_task_group
from write import write_to_gcs_task_group
from staging import gcs_to_staging_task_group
from transform_load_dwh import transform_load_dwh_task_group
from airflow.operators.dummy import DummyOperator

@dag(
    #schedule_interval='@daily',
    schedule_interval=None,
    description='Daily Extraction to GCS',
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False
)
def final_dag():
    start = DummyOperator(task_id="start")

    with TaskGroup("extract", prefix_group_id=False) as section_1:
        extract_data_task_group()

    with TaskGroup("write_to_gcs", prefix_group_id=False) as section_2:
        write_to_gcs_task_group()

    with TaskGroup("gcs_to_staging", prefix_group_id=False) as section_3:
        gcs_to_staging_task_group()

    with TaskGroup("transform_load_dwh", prefix_group_id=False) as section_4:
        transform_load_dwh_task_group()

    end = DummyOperator(task_id='end')

    start >> section_1 >> section_2 >> section_3 >> section_4 >> end
final = final_dag()