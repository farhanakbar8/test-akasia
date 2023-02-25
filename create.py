from sqlalchemy import create_engine, MetaData, ForeignKey, Table, Column, Integer, String, Date
import argparse

def create_table(engine, metadata):
    Employee = Table(
        'Employee', metadata,
        Column('Id', Integer, nullable=False, primary_key = True),
        Column('EmployeeId', String(10), nullable=False, unique=True),
        Column('FullName', String(100), nullable=False),
        Column('BirthDate', Date, nullable=False),
        Column('Address', String(500))
    )
    
    PositionHistory = Table(
        'PositionHistory', metadata,
        Column('Id', Integer, nullable=False, primary_key = True),
        Column('PosId', String(10), nullable=False),
        Column('PosTitle', String(100), nullable=False),
        Column('EmployeeId', String(10), ForeignKey("Employee.EmployeeId"), nullable=False),
        Column('StartDate', Date, nullable=False),
        Column('EndDate', Date, nullable=False)
    )

    metadata.create_all(engine)

def main(params):
    username = params.username
    password = params.password
    host = params.host
    port = params.port
    db = params.db

    engine = create_engine(f'postgresql://{username}:{password}@{host}:{port}/{db}', echo=True)
    metadata = MetaData()

    create_table(engine, metadata)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Script to create table')

    parser.add_argument('--username', help='username for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='db for postgres')

    args = parser.parse_args()

    main(args)
