from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.providers.postgres.hooks.postgres import PostgresHook
from helper.minio import CustomMinio
import logging
import pandas as pd
import json
from airflow.models import Variable
from datetime import timedelta


class Extract:
    @staticmethod
    def _pacflight_db(table_name, incremental, **kwargs):
        """
        Extract all data from Pacflight database (non-incremental).

        Args:
            table_name (str): Name of the table to extract data from.
            **kwargs: Additional keyword arguments.

        Raises:
            AirflowException: If failed to extract data from Pacflight database.
            AirflowSkipException: If no data is found.
        """
        # log start of extraction
        logging.info(f"[Extract] Starting extraction for table: {table_name}")

        ti = kwargs["ti"]                  # get task instance
        ds = kwargs["ds"]                 # get execution date string
                
        try:
            # connect to Pacflight Postgres
            pg_hook = PostgresHook(postgres_conn_id='pacflight_db')
            conn = pg_hook.get_conn()
            cursor = conn.cursor()

            # choose query based on incremental flag
            if incremental:
                date = ds
                query = f"""
                    SELECT * FROM bookings.{table_name}
                    WHERE created_at::DATE = '{date}'::DATE - INTERVAL '1 DAY' 
                    OR updated_at::DATE = '{date}'::DATE - INTERVAL '1 DAY';
                """
                # set object path with yesterday's date
                object_name = f'/temp/{table_name}-{(pd.to_datetime(date) - timedelta(days=1)).strftime("%Y-%m-%d")}.csv'
            else:
                query = f"SELECT * FROM bookings.{table_name};"
                object_name = f'/temp/{table_name}.csv'

            logging.info(f"[Extract] Executing query: {query}")
            cursor.execute(query)
            rows = cursor.fetchall()

            # prepare DataFrame
            columns = [desc[0] for desc in cursor.description]
            cursor.close()
            conn.commit()
            conn.close()

            df = pd.DataFrame(rows, columns=columns)

            # skip if no data found
            if df.empty:
                logging.warning(f"[Extract] Table {table_name} is empty. Skipping...")
                ti.xcom_push(key="return_value", value={"status": "skipped", "data_date": ds})
                raise AirflowSkipException(f"[Extract] Skipped {table_name} — no new data.")

            # serialize JSON fields to strings
            if table_name == 'aircrafts_data':
                df['model'] = df['model'].apply(lambda x: json.dumps(x) if pd.notnull(x) else None)
            if table_name == 'airports_data':
                df['airport_name'] = df['airport_name'].apply(lambda x: json.dumps(x) if pd.notnull(x) else None)
                df['city'] = df['city'].apply(lambda x: json.dumps(x) if pd.notnull(x) else None)
            if table_name == 'tickets':
                df['contact_data'] = df['contact_data'].apply(lambda x: json.dumps(x) if x else None)

            # convert NaN to None for CSV compatibility
            if table_name == 'flights':
                df = df.replace({float('nan'): None})

            # upload DataFrame to MinIO as CSV
            bucket = 'extracted-data'
            logging.info(f"[Extract] Uploading to bucket {bucket}, object {object_name}")
            CustomMinio._put_csv(df, bucket, object_name)

            logging.info(f"[Extract] Extraction completed for table: {table_name}")
            return {"status": "success", "data_date": ds}

        except AirflowSkipException as skip_err:
            logging.warning(f"[Extract] Skipped extraction for {table_name}: {str(skip_err)}")
            raise skip_err
        except Exception as exc:
            logging.error(f"[Extract] Failed extracting {table_name}: {str(exc)}")
            raise AirflowException(f"Error extracting {table_name}: {str(exc)}")
