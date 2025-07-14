from airflow.decorators import task_group
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from flights_staging_pipeline.tasks.components.extract import Extract
from flights_staging_pipeline.tasks.components.load import Load
from airflow.datasets import Dataset

# Define the extract task group
@task_group
def extract(incremental):            
    # Retrieve list of tables to extract
    tables_to_extract = eval(Variable.get('tables_to_extract'))

    for table_name in tables_to_extract:
        current_task = PythonOperator(
            task_id=f'{table_name}',  # use table name as task ID
            python_callable=Extract._pacflight_db,  # function to perform extraction
            trigger_rule='none_failed',  # proceed only if no upstream failures
            op_kwargs={
                'table_name': table_name,
                'incremental': incremental
            }
        )

        current_task  # ensure task is registered

# Define the load task group
@task_group
def load(incremental):
    # Specify load order for staging tables
    tables_load_order = [
        "aircrafts_data",
        "airports_data",
        "bookings",
        "flights",
        "seats",
        "tickets",
        "ticket_flights",
        "boarding_passes"
    ]

    # Retrieve primary key mappings for load operations
    table_pkey = eval(Variable.get("tables_to_load"))
    previous_task = None

    for table_name in tables_load_order:
        current_task = PythonOperator(
            task_id=f"{table_name}",  # use table name as task ID
            python_callable=Load._pacflight_db,  # function to perform load
            trigger_rule="none_failed",  # proceed only if no upstream failures
            # outlets=[Dataset(f'postgres://warehouse_pacflight:5432/warehouse_pacflight.stg.{table_name}')],
            op_kwargs={
                "table_name": table_name,
                "table_pkey": table_pkey,
                "incremental": incremental
            }
        )

        # Chain tasks in specified order
        if previous_task:
            previous_task >> current_task

        previous_task = current_task  # update reference for next iteration
