from prefect_gcp.cloud_storage import GcsBucket
from prefect import flow, task
from prefect_sqlalchemy import SqlAlchemyConnector
from prefect_gcp import GcpCredentials
import pandas as pd

@task()
def gcs_to_local(retries=3):
    gcs_path_employee ='employee.parquet'
    gcs_path_position ='position.parquet'
    local_path_employee = '../../data/'
    local_path_position = '../../data/'
    gcs_block = GcsBucket.load('akasia-gcs')
    gcs_block.get_directory(from_path=gcs_path_employee, local_path=local_path_employee)
    gcs_block.get_directory(from_path=gcs_path_position, local_path=local_path_position)
    
    return local_path_employee + 'employee.parquet', local_path_position + 'position.parquet'

@task(log_prints=True, retries=3)
def write_to_bq(path_employee, path_position):
    gcp_credentials_block = GcpCredentials.load("akasia-gcp-creds")

    df_employee = pd.read_parquet(path_employee)
    df_position = pd.read_parquet(path_position)

    df_employee.to_gbq(
        destination_table='akasia_dataset.employee',
        project_id='data-engineer-course-377607',
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        if_exists='replace'
    )

    df_position.to_gbq(
        destination_table='akasia_dataset.position',
        project_id='data-engineer-course-377607',
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        if_exists='replace'
    )

@flow(name='ETL from Gcs Bucket to BigQuery')
def etl_gcs_to_bq():
    path_employee, path_position = gcs_to_local()
    write_to_bq(path_employee, path_position)


if __name__ == "__main__":
    etl_gcs_to_bq()