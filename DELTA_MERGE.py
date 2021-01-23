from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import pyodbc
import pandas as pd
import logging
import time
import connections

#log config
logging.basicConfig(filename='DATA_MIGRATION_DAB.log', filemode='a',format='%(asctime)s - %(levelname)s - %(message)s ', datefmt='%d-%b-%y %H:%M:%S', level=logging.INFO)

def DeltaMerge(table, interval):
   
    
    #///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    #|||||||||||||||||||||||||||||||||||||||||  - SETUP  -  ||||||||||||||||||||||||||||||||||||||||||||||||||
    #///////////////////////////////////////////////////////////////////////////////////////////////////////////////////


    #---specifiy data tables to manipulate---
    
    if "VehicleSales" in table: #if vehiclesales is in the string then set the postgres table we are pulling from to vehicle sales
        PostgreTable = "vehiclesales"
    else: #if not vehicle sales continue as normal
        PostgreTable = table.lower()
        
    MSSQL_Table = table
    
    logging.info(f'------------------------ Starting DELTA MERGE for {table} -------------------------------------\n')
    print(f'------------------------------ Starting for {table} --------------------------------------------\n')

    #connections
    dbpg = connections.PGconnection()

    dbmssql = connections.MSconnection()


    #///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    #|||||||||||||||||||||||||||||||||||||||||  - STAGE AND LOAD INTO DELTA TABLE  -  ||||||||||||||||||||||||||||||||||
    #///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    
    #Setup Data Frames and test postgres connection
    try:
        testConnectivity = pd.read_sql(f'SELECT * FROM {PostgreTable} where rowlastupdated >= NOW() - INTERVAL \'{interval} HOURS\' limit 1', dbpg)  #run fast postgresql query to test connectivitiy 
    except Exception as e:
        logging.error('Could not connect to PostgreSQL', exc_info=True) 
    try:
        mssqlDelta_df = pd.read_sql(f'SELECT TOP(1) * FROM DELTA_{MSSQL_Table}', dbmssql) #get top row from DELTA MSSQL table for schema info
    except Exception as e:
        logging.error('Could not Connect to MMSQL', exc_info=True)

    mssqlMain_df = pd.read_sql(f'SELECT TOP(1) * FROM {MSSQL_Table}', dbmssql) #get top row from main MSSQL table for schema info


    #truncate mssql DELTA table and load new data from postgres into DELTA TABLE
    Session = sessionmaker(bind=dbmssql)
    session = Session()
    session.execute(f'Truncate Table DELTA_{MSSQL_Table}')
    session.commit()
    session.close()
    logging.info(f'DELTA_{table} Truncated, ready for staging')

    
    #break postgresql data into chunks for better memory usage and load in one chunk at a time to DELTA MSSQL tabel
    for dfpg in (pd.read_sql(f'SELECT * FROM {PostgreTable} where rowlastupdated >= NOW() - INTERVAL \'{interval} HOURS\'', dbpg, chunksize = 5000)): #get data from postgresql based on interval 
        

        
        #compare schema and alter dataframe based on schema (remove any unused coloumns from postgres datatframe)
        MSSQLschema = mssqlDelta_df.columns
        pgschema = dfpg.columns
        column_diff = pgschema.difference(MSSQLschema)
        #remove unused columns from postgresql dataframe
        for column in column_diff:
            del dfpg[column]
            
        try:
            dfpg.to_sql(f'DELTA_{MSSQL_Table}', dbmssql, if_exists='append', index=False)
            print('adding data to delta')
        except Exception as e:
            #log error if unable to merge data
            print(f'Unable to load chunk into DELTA_{table}')
            logging.error(f'Unable to load chunk into DELTA_{table}', exc_info=True)
            pass
        else:
            logging.info(f'Data Chunk Staged now replicating to DELTA_{table}...')
            print(f'Data Chunk Staged now replicating to DELTA_{table}...')
            

                
            
    #///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    #|||||||||||||||||||||||||||||||||||||||||  - MERGE DELTA TO MAIN -  |||||||||||||||||||||||||||||||||||||||||||||||
    #///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    logging.info(f'DELTA_{table} ready building merge statement...')
    print(f'DELTA_{table} ready building merge statement...')


    #--------get columns from MAIN table to help build merge statement---------------
            
    MSSQLschema = mssqlMain_df.columns
    Delta = mssqlDelta_df.columns
            
    column_diff = MSSQLschema.difference(Delta)#make sure columns match before merging
    for column in column_diff:
        print(column)
        del mssqlMain_df[column]
                
    MSSQLschema = mssqlMain_df.columns

    #build strings for sql statements using columns    
    OGcolumns = ' '.join([str(f',[{column}]') for column in MSSQLschema]) 
    Scolumns = ' '.join([str(f',s.[{column}]') for column in MSSQLschema])
    CompareColumns = ' '.join([str(f',t.[{column}] = s.[{column}]') for column in MSSQLschema])
            
    #remove excess comma from strings to prevent syntax error in sql
    OGcolumns = ''.join(OGcolumns.split(',', 1))
    Scolumns = ''.join(Scolumns.split(',', 1))
    CompareColumns = ''.join(CompareColumns.split(',', 1))

            
    #------------------------------set primary keys to merge on------------------------------------------------------
            
    MSSQLPkeys = pd.read_sql(f'EXEC sp_pkeys {MSSQL_Table}', dbmssql)#get primary keys from sql table
    pkeys = ' '.join([str(f's.[{column}] = t.[{column}] and') for column in MSSQLPkeys['COLUMN_NAME']]) #build merge on statement for sql query
            
    if not pkeys: #set static merge on if no primary keys are set in sql
        pkeys = 's.[hostitemid] = t.[hostitemid] and s.[cora_acct_id] = t.[cora_acct_id]'
    else:#remove excess "and" key word to prevent syntax error in sql
        pkeys = pkeys.rsplit(' ', 1)[0]

    #--------------------------------------------------------Build SQL Merge----------------------------
                
    #try updating columns and adding new ones when not matched
    SQL_merge_withMain_columns = """

                MERGE [dbo].{TABLE} t 
                   USING [dbo].DELTA_{TABLE} s
                ON ({pkeys})
                 
                WHEN MATCHED
                    THEN UPDATE SET 
                      {CompareColumns}

                WHEN NOT MATCHED BY TARGET 
                    THEN INSERT (
                      {columns}
                    )
                   VALUES (
                       {scolumns}
                   );

        """.format(TABLE=MSSQL_Table, columns=OGcolumns, scolumns=Scolumns, CompareColumns=CompareColumns, pkeys=pkeys)
            
    #if no primary keys set and hostitemid/corra__acct_id not there, use first two columns to merge on
    megreOn = f's.[{MSSQLschema[0]}] = t.[{MSSQLschema[0]}] and s.[{MSSQLschema[1]}] = t.[{MSSQLschema[1]}]'
    SQL_merge_withNo_pkeys = """

            MERGE [dbo].{TABLE} t 
                USING [dbo].DELTA_{TABLE} s
            ON ({mergeon})
                 
            WHEN MATCHED
                THEN UPDATE SET 
                    {CompareColumns}

            WHEN NOT MATCHED BY TARGET 
                THEN INSERT (
                    {columns}
                )
                VALUES (
                    {scolumns}
                );

        """.format(TABLE=MSSQL_Table, columns=OGcolumns, scolumns=Scolumns, CompareColumns=CompareColumns, mergeon=megreOn)
            
    #--------------merge data from MSSQL delta using sql command-------------
            
    logging.info(f'Merging Data...')
    print(f'Merging Data...')
    
    Session = sessionmaker(bind=dbmssql)
    session = Session()
    
    try:
        #try merging based on main table schema
        session.execute(SQL_merge_withMain_columns)
        session.commit()
    except AssertionError:
        #try merging using with first two columns as primary keys
        session.execute(SQL_merge_withNo_pkeys)
        session.commit()
    except Exception as e:
        #log error if unable to merge data
        logging.error(f'Unable to merge Delta to {table}', exc_info=True)
        dbpg.dispose()
        session.close()
    else:
        dbpg.dispose()
        session.close()
        time.sleep(15)    
        logging.info(f'Data Merged to main {table}\n')
        print(f'Data Merged to main {table}')
          