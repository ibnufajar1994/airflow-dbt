from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd

BASE_PATH = "/opt/airflow/dags"


class Execute:
    @staticmethod
    def _query(connection_id, query_path):
        """
        Execute a SQL file against a Postgres connection without returning data.
        """
        # initialize Postgres hook and open connection
        hook = PostgresHook(postgres_conn_id=connection_id)
        conn = hook.get_conn()
        cursor = conn.cursor()

        # read SQL statement from file
        with open(f'{BASE_PATH}/{query_path}', 'r') as file:
            query = file.read()

        # run the SQL command
        cursor.execute(query)
        # close resources after execution
        cursor.close()
        conn.commit()
        conn.close()

    @staticmethod
    def _get_dataframe(connection_id, query_path):
        """
        Run a SQL query and return the results as a pandas DataFrame.
        """
        # connect to Postgres
        hook = PostgresHook(postgres_conn_id=connection_id)
        conn = hook.get_conn()
        cursor = conn.cursor()

        # load query from file
        with open(f'{BASE_PATH}/{query_path}', 'r') as file:
            query = file.read()

        # execute query and fetch all rows
        cursor.execute(query)
        rows = cursor.fetchall()
        # extract column names from cursor metadata
        columns = [desc[0] for desc in cursor.description]
        # build DataFrame
        df = pd.DataFrame(rows, columns=columns)

        # cleanup connection
        cursor.close()
        conn.commit()
        conn.close()

        return df

    @staticmethod
    def _insert_dataframe(connection_id, query_path, dataframe):
        """
        Insert rows from a DataFrame into Postgres using a parameterized SQL template.
        """
        # load SQL insert template
        with open(f'{BASE_PATH}/{query_path}', 'r') as file:
            query = file.read()

        # set up Postgres hook for executing inserts
        hook = PostgresHook(postgres_conn_id=connection_id)
        conn = hook.get_conn()
        cursor = conn.cursor()

        # iterate over DataFrame rows and execute insert for each record
        for _, row in dataframe.iterrows():
            params = row.to_dict()
            hook.run(query, parameters=params)

        # finalize transaction and close
        cursor.close()
        conn.commit()
        conn.close()
