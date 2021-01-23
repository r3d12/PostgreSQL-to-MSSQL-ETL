from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import pyodbc
from configparser import ConfigParser

file = 'config.ini'
config = ConfigParser()
config.read(file)


def PGconnection():
   
    pgServer = config['postgresql']['server1']
    pgUser = config['postgresql']['user1']
    pgPass = config['postgresql']['pass1']
    
    db_string_postgres = f'postgres://{pgUser}:{pgPass}@{pgServer}?client_encoding=latin-1'

    engine = create_engine(db_string_postgres)
    return engine

def MSconnection():

    msServer = config['mssql']['server']
    msUser = config['mssql']['user']
    msPass = config['mssql']['pass']

    db_string_mssql = f'mssql+pyodbc://{msUser}:{msPass}@{msServer}?driver=SQL Server?Trusted_Connection=yes'

    engine = create_engine(db_string_mssql)
    return engine