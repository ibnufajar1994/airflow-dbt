import logging
from pathlib import Path
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.exceptions import AirflowException


class Transform:
    @staticmethod
    def build_operator(task_id: str, table_name: str, sql_dir: str = "flights_data_pipeline/query/final"):
        """
        Build a PostgresOperator to run the transformation SQL for a specified table.
        """
        # construct full path to SQL file
        query_path = f"/opt/airflow/dags/{sql_dir}/{table_name}.sql"

        try:
            # load SQL query from file system
            sql_content = Path(query_path).read_text()
        except FileNotFoundError:
            # raise if the SQL file is missing
            raise AirflowException(f"[Transform] SQL file not found for table: {table_name}")

        logging.info(f"[Transform] Preparing operator for table: {table_name}")  # log operator creation
        # return operator to execute the loaded SQL against staging DB
        return PostgresOperator(
            task_id=task_id,
            postgres_conn_id='warehouse_pacflight',
            sql=sql_content
        )
