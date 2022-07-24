
import pandas as pd
import numpy as np

print(f"!!! WARNING !!!")
print(f"Don't use on existing tables filled with data. Use {__name__} on your own risk!")

def csv_to_CH_autoload(file_path: str, db: str, table: str, client_CH, verbose: int = 1):
    ''' Auto loading CSV file to Clickhose, with column type detection and table creatind DDL.

    !!! WARNING TABLE db.table WILL BE DROPED !!!
    
    Return transformed pandas dataframe with autodetected data types
    Support types Datetime, Float, Int, String

    file_path   -   path to CSV.file 
    db          -   database name in Clickhouse (database should be allready created)
    table       -   new tables name in Clickhouse
    client_CH   -   initialised client from 'clickhouse_driver' library  
        client_CH = Client('localhost', settings={'use_numpy': True}):
        setting {'use_numpy': True} is mandatory
    verbose     -   quantity of output print information

    '''
    # loadindg data from file to pandas dataframe
    df = pd.read_csv(file_path)

    # detecting types
    col_names = df.columns


    # trying to transform to datetime column
    for col in col_names:
        if df[col].dtype == 'object':
            try:
                np_col = pd.to_datetime(df[col])
                if (np_col > np.datetime64('1980')).all() and (np_col < np.datetime64('2025')).all():  # sanity check for most popular datetimes
                    df[col] = np_col
            except ValueError:
                next

            
    # trying to transform to int column
    for col in col_names:
        if df[col].dtype == 'object':
            if all(map(lambda x: str(x).lstrip('-').isdigit(), df[col])):
                try:
                    df[col] = df[col].astype('long')
                except ValueError:
                    next

    # trying to transform to float column
    for col in col_names:
        if df[col].dtype == 'object':
            if all(map(lambda x: str(x).lstrip('-').replace('.', '').isnumeric(), df[col])):
                try:
                    df[col] = df[col].astype('float')
                except ValueError:
                    next

    if verbose > 1: print(df.dtypes)


    # drop table if exist
    sql_drop = f"DROP TABLE IF EXISTS {db}.{table};"
    if verbose > 1: print(sql_drop)
    out = ""
    out = client_CH.execute(sql_drop) 
    print("Result of drop: ", out)

    # creating table
    columns_list = []
    for col, dtype in df.dtypes.to_dict().items():
        if 'datetime' in str(type(dtype)).lower():
            new_col = 'Datetime'
        elif 'long' in str(type(dtype)).lower():
            new_col = 'Int64'
        elif 'int64' in str(type(dtype)).lower():
            new_col = 'Int64'
        elif 'float' in str(type(dtype)).lower():
            new_col = 'Float32'
        elif 'bool' in str(type(dtype)).lower():
            new_col = 'Boolean'
        else:
            new_col = 'LowCardinality(String)'
        columns_list.append((col, new_col))
    colums_clause = ',\n\t\t'.join([x[0] + ' ' + x[1] for x in columns_list])
    sql_create = sql_create = f"""
        CREATE TABLE IF NOT EXISTS {db}.{table} (
            {colums_clause}
        )
        ENGINE = MergeTree()
        ORDER BY tuple();
        """
    if verbose > 1: print(sql_create)
    out = ""
    out = client_CH.execute(sql_create) 
    print("Result of creating table: ", out)

    # inserting data into Clickhouse
    sql_insert = f"INSERT INTO {db}.{table} VALUES"
    if verbose > 1: print(sql_insert)
    out = client_CH.insert_dataframe(sql_insert, df)
    print("Result of Insert: ", out)

    return df