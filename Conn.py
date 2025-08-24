import pyodbc
import os
from dotenv import load_dotenv

load_dotenv()
os.getenv("OPENAI_ENDPOINT")

def connect_db():
    conn = pyodbc.connect(
        f"DRIVER={os.getenv('DB_DRIVER')};"
        f"SERVER={os.getenv('DB_SERVER')};"
        f"DATABASE={os.getenv('DB_DATABASE')};"
        f"UID={os.getenv('DB_UID')};"
        f"PWD={os.getenv('DB_PWD')};"
        r'TrustServerCertificate=yes;'
    )
    return conn



# def connect_db():
#     conn = pyodbc.connect(
#         r'DRIVER={ODBC Driver 17 for SQL Server};'
#         r'SERVER=mihcm-tenant-sg.database.windows.net;'
#         r'DATABASE=Demo_ENTLK;'
#         r'UID=mihcmropowerbi;'
#         r'PWD=ndCXAzO@%OTp;'
#         r'TrustServerCertificate=yes;'
#     )
#     return conn

def execute_sql(query):
    conn = connect_db()
    print("Executing query:", query)
    cursor = conn.cursor()
    cursor.execute(query)
    columns = [column[0] for column in cursor.description]
    rows = cursor.fetchall()
    return [dict(zip(columns, row)) for row in rows]

print("Data retrieved")
