from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowSkipException, AirflowException
from helper.minio import CustomMinio
from pangres import upsert
from sqlalchemy import create_engine
from datetime import timedelta
import logging
import pandas as pd


class Load:
    @staticmethod
    def _pacflight_db(table_name, incremental, **kwargs):
        """
        Load CSV data from MinIO into staging schema of PostgreSQL.
        """
        # indicate start of loading process
        logging.info(f"[Load] Starting load for table: {table_name}")
        date = kwargs.get("ds")      # execution date
        ti = kwargs["ti"]            # task instance for XCom interaction

        # retrieve extract status from prior task
        extract_result = ti.xcom_pull(task_ids=f"extract.{table_name}")
        if extract_result is None:
            logging.warning(f"[Load] No extract result for {table_name}, skipping.")
            raise AirflowSkipException(f"[Load] Skipped {table_name} due to missing extract result.")

        logging.info(f"[Load] Extract output: {extract_result}")

        # skip if extract did not succeed
        if extract_result.get("status") != "success":
            logging.info(f"[Load] Extract status not success for {table_name}, skipping.")
            raise AirflowSkipException(f"[Load] Skipped {table_name} because extract status was {extract_result.get('status')}")

        table_pkey = kwargs.get("table_pkey")
        # determine CSV object path based on mode
        object_date = (pd.to_datetime(date) - timedelta(days=1)).strftime("%Y-%m-%d")
        object_name = f"/temp/{table_name}-{object_date}.csv" if incremental else f"/temp/{table_name}.csv"
        bucket_name = "extracted-data"

        # create SQLAlchemy engine for staging DB
        engine = create_engine(PostgresHook(postgres_conn_id="warehouse_pacflight").get_uri())

        try:
            logging.info(f"[Load] Fetching CSV {object_name} from bucket {bucket_name}")
            df = CustomMinio._get_dataframe(bucket_name, object_name)

            if df.empty:
                logging.warning(f"[Load] Downloaded data is empty for {table_name}, skipping.")
                ti.xcom_push(key="return_value", value={"status": "skipped", "data_date": date})
                raise AirflowSkipException(f"[Load] Skipping {table_name}: no rows in CSV.")

            # index DataFrame by primary key(s)
            df = df.set_index(table_pkey[table_name])

            # upsert rows into staging schema
            upsert(
                con=engine,
                df=df,
                table_name=table_name,
                schema="stg",
                if_row_exists="update"
            )

            logging.info(f"[Load] Successfully loaded {len(df)} records into {table_name}.")
            ti.xcom_push(key="return_value", value={"status": "success", "data_date": date})

        except AirflowSkipException as skip_exc:
            logging.warning(str(skip_exc))
            raise skip_exc

        except Exception as exc:
            logging.error(f"[Load] Error loading {table_name}: {exc}")
            raise AirflowException(f"[Load] Failed to load {table_name}: {exc}")

        finally:
            engine.dispose()  # close DB connections
