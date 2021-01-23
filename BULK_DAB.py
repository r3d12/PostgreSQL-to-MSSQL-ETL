from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import pyodbc
import pandas as pd
import logging
import connections

logging.basicConfig(filename='DATA_MIGRATION_DAB.log', filemode='a',format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S', level=logging.INFO)

def BULK(table):
    #specifiy data tables to manipulate

    PostgreTable = table.lower()
    MSSQL_Table = table

    logging.info(f'------------------------------ Starting BULK update for {table} -------------------------------------------------------\n')
    
    #connections
    dbpg = connections.PGconnection()

    dbmssql = connections.MSconnection()
    
    #Data Frames & connecttion tests
    try:
        dfpg = pd.read_sql(f'SELECT * FROM {PostgreTable} limit 1', dbpg)
    except Exception as e:
        logging.error('Could not connect to PostgreSQL')
    try:
        mssql_df = pd.read_sql(f'SELECT TOP(1) * FROM {MSSQL_Table}', dbmssql)
    except:
        logging.error('Could not Connect to MMSQL')
        
 
    #truncate mssql table and load new data from postgres
    try:   
        Session = sessionmaker(bind=dbmssql)
        session = Session()
        session.execute(f'Truncate Table {MSSQL_Table}')
        session.commit()
        session.close()
    except:
        logging.error(f'unabel to truncate {table}')
    logging.info(f'Data Staged now truncating then replicating to {table}...')

    #load data into chuncks for better memory utilization and monitoring
    for dfpg in (pd.read_sql(f'SELECT * FROM {PostgreTable}', dbpg, chunksize = 5000)):
        #compare schema and alter dataframe based on schema (remove any unused coloumns from postgres dtatframe)
        MSSQLschema = mssql_df.columns
        pgschema = dfpg.columns
        column_diff = pgschema.difference(MSSQLschema)
        for column in column_diff:
            del dfpg[column]

        try:
            dfpg.to_sql(MSSQL_Table, dbmssql, if_exists='append', index=False)
            print(f'Data chunck from postgres copied to {table}')
            logging.info(f'Data Chunck from postgres copied to {table}')
        except Exception as e:
            print(f'could not replicate postgres table to {table}', exc_info=True)
            logging.error(f'Could not replicate postgres table to {table}', exc_info=True)
        else:
            dbpg.dispose()
            logging.info(f'Data from postgres copied to {table}\n')

    
    
    
