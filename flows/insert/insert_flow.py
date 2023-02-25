from sqlalchemy import create_engine
import argparse
from prefect import task, flow
from prefect_sqlalchemy import SqlAlchemyConnector

@task(log_prints=True, retries=3)
def insert_data(engine):
    employee_data = [
        {'Id':1, 'EmployeeId':'10105001', 'FullName':'Ali Anton', 'BirthDate':'19-Aug-82', 'Address':'Jakarta Utara'},
        {'Id':2, 'EmployeeId':'10105002', 'FullName':'Rara Siva', 'BirthDate':'1-Jan-82', 'Address':'Mandalika'},
        {'Id':3, 'EmployeeId':'10105003', 'FullName':'Rini Aini', 'BirthDate':'20-Feb-82', 'Address':'Sumbawa Besar'},
        {'Id':4, 'EmployeeId':'10105004', 'FullName':'Budi Jiwa', 'BirthDate':'22-Feb-82', 'Address':'Mataram Kota'},
        {'Id':5, 'EmployeeId':'10105005', 'FullName':'Farhan Nur', 'BirthDate':'25-Feb-02', 'Address':'Kota Jambi'},
    ]
    
    
    for item in employee_data:
        engine.execute(f"insert into \"Employee\" (\"Id\", \"EmployeeId\", \"FullName\", \"BirthDate\", \"Address\") \
                        values ({item['Id']}, \'{item['EmployeeId']}\', \
                        \'{item['FullName']}\', \'{item['BirthDate']}\', \'{item['Address']}\')")
    
    position_data = [
        {'Id':1, 'PosId': '50000', 'PosTitle': 'IT Manager', 'EmployeeId':'10105001', 'StartDate':'1-Jan-2022', 'EndDate':'28-Feb-2022'},
        {'Id':2, 'PosId': '50001', 'PosTitle': 'IT Sr. Manager', 'EmployeeId':'10105001', 'StartDate':'1-Mar-2022', 'EndDate':'31-Dec-2022'},
        {'Id':3, 'PosId': '50002', 'PosTitle': 'Programmer Analyst', 'EmployeeId':'10105002', 'StartDate':'1-Jan-2022', 'EndDate':'28-Feb-2022'},
        {'Id':4, 'PosId': '50003', 'PosTitle': 'Sr. Programmer Analyst', 'EmployeeId':'10105002', 'StartDate':'1-Mar-2022', 'EndDate':'31-Dec-2022'},
        {'Id':5, 'PosId': '50004', 'PosTitle': 'IT Admin', 'EmployeeId':'10105003', 'StartDate':'1-Jan-2022', 'EndDate':'28-Feb-2022'},
        {'Id':6, 'PosId': '50005', 'PosTitle': 'IT Secretary', 'EmployeeId':'10105003', 'StartDate':'1-Mar-2022', 'EndDate':'31-Dec-2022'},
        {'Id':7, 'PosId': '50006', 'PosTitle': 'Data Analyst', 'EmployeeId':'10105004', 'StartDate':'1-Jan-2022', 'EndDate':'28-Feb-2022'},
        {'Id':8, 'PosId': '50007', 'PosTitle': 'Sr. Data Analyst', 'EmployeeId':'10105004', 'StartDate':'1-Mar-2022', 'EndDate':'31-Dec-2022'},
        {'Id':9, 'PosId': '50008', 'PosTitle': 'Data Engineer', 'EmployeeId':'10105005', 'StartDate':'1-Jan-2022', 'EndDate':'28-Feb-2022'},
        {'Id':10, 'PosId': '50009', 'PosTitle': 'Sr. Data Engineer', 'EmployeeId':'10105005', 'StartDate':'1-Mar-2022', 'EndDate':'31-Dec-2022'}
    ]

    for item in position_data:
        engine.execute(f"insert into \"PositionHistory\" (\"Id\", \"PosId\", \"PosTitle\", \"EmployeeId\", \"StartDate\", \"EndDate\") \
                        values ({item['Id']}, \'{item['PosId']}\', \'{item['PosTitle']}\', \'{item['EmployeeId']}\', \'{item['StartDate']}\', \
                        \'{item['EndDate']}\')")

@flow(name="Insert Data Flow")
def main():

    connection_block = SqlAlchemyConnector.load("postgresql-connector")
    with connection_block.get_connection(begin=False) as engine:
        insert_data(engine)

if __name__ == "__main__":
    main()
