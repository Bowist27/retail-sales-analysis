from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from datetime import datetime
import pandas as pd
import sqlite3

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 0,
}

DATA_DIR = '/app/data'

with DAG(
    dag_id='etl_pipeline',
    default_args=default_args,
    schedule_interval='@once',
    catchup=False,
) as dag:

    def extract_data_func():
        """Simula la extracción: crea datos de ejemplo."""
        data = {'product_id': [1, 2, 3, 4],
                'name': ['Product A', 'Product B', None, 'Product D'],
                'price': [10, 20, 15, 25]}
        df = pd.DataFrame(data)
        extracted_data_path = f"{DATA_DIR}/extracted_data.csv"
        df.to_csv(extracted_data_path, index=False)
        print(f"Data extracted to {extracted_data_path}")

    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data_func,
    )

    def transform_data_func():
        """Limpia y transforma los datos."""
        extracted_data_path = f"{DATA_DIR}/extracted_data.csv"
        transformed_data_path = f"{DATA_DIR}/transformed_data.csv"
        df = pd.read_csv(extracted_data_path)
        df['name'] = df['name'].fillna('Unknown Product')
        df['discounted_price'] = df['price'] * 0.9
        df.to_csv(transformed_data_path, index=False)
        print(f"Data transformed and saved to {transformed_data_path}")

    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data_func,
    )

    def load_data_func():
        """Carga los datos transformados en SQLite."""
        db_path = f"{DATA_DIR}/demo.db"
        transformed_data_path = f"{DATA_DIR}/transformed_data.csv"
        conn = sqlite3.connect(db_path)
        df = pd.read_csv(transformed_data_path)
        df.to_sql('products', conn, if_exists='replace', index=False)
        conn.close()
        print(f"Data loaded into 'products' table in {db_path}")

    load_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data_func,
    )

    validate_task = SqliteOperator(
        task_id='validate_data',
        sqlite_conn_id='my_sqlite_conn',
        sql="SELECT COUNT(*) FROM products;",
    )

    extract_task >> transform_task >> load_task >> validate_task
