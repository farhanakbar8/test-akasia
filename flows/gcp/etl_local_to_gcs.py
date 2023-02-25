from prefect_gcp.cloud_storage import GcsBucket
from prefect import flow, task
from prefect_sqlalchemy import SqlAlchemyConnector
import pandas as pd
import gspread as gs

@task()
def postgres_to_local(conn):
    results = conn.fetch_all("select * from \"Employee\"")
    df = pd.DataFrame(results)
    df.to_parquet('employee.parquet')

@task()
def spreadsheet_to_local(account):
    file_ = account.open_by_url('https://docs.google.com/spreadsheets/d/1UOyGRO5bU8ZwRE9x3evTduAnlH5d6mnXps2OlA9-tdY/edit?usp=sharing')
    sheet = file_.worksheet('PositionHistory')
    df = pd.DataFrame(sheet.get_all_records())
    df.to_parquet('position.parquet')

@task(log_prints=True, retries=3)
def write_gcs(path_employee, path_position):
    gcs_block = GcsBucket.load('akasia-gcs')
    gcs_block.upload_from_path(from_path=path_employee, to_path=path_employee)
    gcs_block.upload_from_path(from_path=path_position, to_path=path_position)

@flow(name='ETL from Local to GCS Bucket')
def etl_local_to_gcs():
    with SqlAlchemyConnector.load("postgresql-connector") as conn:
        postgres_to_local(conn)
    account = gs.service_account(filename='../../data-engineer-course-377607-f3f67f6dcc40.json')
    spreadsheet_to_local(account)

    write_gcs(path_employee='employee.parquet', path_position='position.parquet')


if __name__ == "__main__":
    etl_local_to_gcs()

