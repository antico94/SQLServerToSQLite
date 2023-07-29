import os
import pandas as pd
from sqlalchemy import create_engine, text
import configparser
import sqlite3

# Read configuration from config.ini file
config = configparser.ConfigParser()
config.read("config.ini")

# SQL Server connection settings
sql_server_server = config.get("SQL_SERVER_DATABASE", "SERVER")
sql_server_database = config.get("SQL_SERVER_DATABASE", "DATABASE")

# SQLite connection settings
sqlite_database_file = config.get("SQLITE_DATABASE", "DATABASE_FILEPATH")
sqlite_connection_string = f"sqlite:///{sqlite_database_file}"


# Function to get the list of tables from SQL Server database
def get_tables_from_sql_server():
    try:
        engine = create_engine(f'mssql+pyodbc://{sql_server_server}/{sql_server_database}?driver=SQL+Server')
        with engine.connect() as connection:
            query = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE';"
            result = connection.execute(text(query))
            table_names = [row.TABLE_NAME for row in result]
        return table_names
    except Exception as e:
        print("Error connecting to SQL Server:", e)
        raise


# Export data from SQL Server to CSV files
def export_data_to_csv():
    engine = create_engine(f'mssql+pyodbc://{sql_server_server}/{sql_server_database}?driver=SQL+Server')
    for table_name in tables_to_transfer:
        query = f"SELECT * FROM {table_name};"
        df = pd.read_sql(query, engine)
        df.to_csv(f"{table_name}.csv", index=False)


# Import data from CSV files into SQLite
def import_data_to_sqlite():
    engine = create_engine(sqlite_connection_string)
    for table_name in tables_to_transfer:
        try:
            df = pd.read_csv(f"{table_name}.csv")
            df.to_sql(table_name, engine, if_exists='replace', index=False)
            print(f"Successfully imported data from {table_name}.csv to SQLite.")
        except Exception as e:
            print(f"Error importing data from {table_name}.csv to SQLite:", e)


if __name__ == "__main__":
    tables_to_transfer = get_tables_from_sql_server()
    print("Tables to transfer:", tables_to_transfer)

    # Create the SQLite file and database if it doesn't exist
    if not os.path.exists(sqlite_database_file):
        conn = sqlite3.connect(sqlite_database_file)
        conn.close()
        print(f"Created new SQLite database: {sqlite_database_file}")

    # Perform data transfer
    export_data_to_csv()
    import_data_to_sqlite()

print("Data transfer completed!")
