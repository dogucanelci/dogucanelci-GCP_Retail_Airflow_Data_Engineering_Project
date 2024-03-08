from airflow import DAG
from datetime import datetime
from airflow.operators.python_operator import PythonOperator

from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

from cosmos.config import ProfileConfig, ProjectConfig
from pathlib import Path
from cosmos.airflow.task_group import DbtTaskGroup
from include.dbt.cosmos_config import DBT_CONFIG,DBT_PROJECT_CONFIG
from cosmos.constants import LoadMode
from cosmos.config import ProjectConfig, RenderConfig

def soda_check_raw_invoices(scan_name='soda_check_raw_invoices', checks_subpath='sources'):
    from include.soda.check_function import check
    return check(scan_name, checks_subpath)

def soda_check_transform(scan_name='soda_check_transform', checks_subpath='transform'):
    from include.soda.check_function import check
    return check(scan_name, checks_subpath)

def soda_check_report(scan_name='soda_check_report', checks_subpath='report'):
    from include.soda.check_function import check
    return check(scan_name, checks_subpath)

with DAG(dag_id='retail1',
         start_date=datetime(2023, 1, 1),
         schedule_interval=None,
         catchup=False) as dag:

    upload_csv_to_gcs = LocalFilesystemToGCSOperator(
        task_id='upload_csv_to_gcs',
        src='include/dataset/online_retail.csv',
        dst='raw/online_retail.csv',
        bucket='online_retail_bucket_dogucanelci',
        gcp_conn_id='gcp',
        mime_type='text/csv',
        dag=dag  
    )

    create_retail_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='create_retail_dataset',
        dataset_id="retail",
        gcp_conn_id="gcp",
    )

    soda_check_raw_invoices = PythonOperator(
    task_id='soda_check_raw_invoices',
    python_callable=soda_check_raw_invoices,
    op_args=['check_load', 'sources'],
    dag=dag
)
    
    soda_check_transform = PythonOperator(
task_id='soda_check_transform',
python_callable=soda_check_transform,
op_args=['soda_check_transform', 'transform'],
dag=dag
)
    
    soda_check_report = PythonOperator(
task_id='soda_check_report',
python_callable=soda_check_report,
op_args=['soda_check_report', 'report'],
dag=dag
)

    transform_model = DbtTaskGroup(
            group_id='transform_model',
            project_config=DBT_PROJECT_CONFIG,
            profile_config=DBT_CONFIG,
            render_config=RenderConfig(
                load_method=LoadMode.DBT_LS,
                select=['path:models/transform']
            )
        )
    
    transform_report = DbtTaskGroup(
            group_id='transform_report',
            project_config=DBT_PROJECT_CONFIG,
            profile_config=DBT_CONFIG,
            render_config=RenderConfig(
                load_method=LoadMode.DBT_LS,
                select=['path:models/report']
            )
        )

projectid = 'gcp-retail-data-eng-proj-no3'
schema_name = 'retail'
table_name = 'raw_invoices'
schema = [
    {"name": "InvoiceNo", "type": "STRING"},
    {"name": "StockCode", "type": "STRING"},
    {"name": "Description", "type": "STRING"},
    {"name": "Quantity", "type": "INTEGER"},
    {"name": "InvoiceDate", "type": "STRING"},
    {"name": "UnitPrice", "type": "FLOAT"},
    {"name": "CustomerID", "type": "STRING"},
    {"name": "Country", "type": "STRING"},
]

create_empty_table_bq = BigQueryCreateEmptyTableOperator(
    task_id='create_empty_table_bq',
	dataset_id='retail',
	table_id='raw_invoices',
	schema_fields=schema,
	gcp_conn_id="gcp",
	google_cloud_storage_conn_id="gcp",)


gcs_to_bq = GCSToBigQueryOperator(
    task_id='gcs_to_bq',
	bucket='online_retail_bucket_dogucanelci',
	source_objects=['raw/online_retail.csv'],
	destination_project_dataset_table=f"{projectid}.{schema_name}.{table_name}",
	schema_fields=schema,
	source_format="CSV",
	field_delimiter=",",
	max_bad_records="0",
    gcp_conn_id="gcp",
    write_disposition="WRITE_TRUNCATE",
)




upload_csv_to_gcs >> create_retail_dataset >> create_empty_table_bq >> gcs_to_bq >> soda_check_raw_invoices >> transform_model \
>> soda_check_transform >> transform_report >> soda_check_report