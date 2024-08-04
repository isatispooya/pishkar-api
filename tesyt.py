import setting 
import pandas as pd
import pyodbc


ip_sql_server = '192.168.10.5'
username_sql_server = 'findev'
password_sql_server = 'Moeen....6168'
port_sql_server = '50068'
database = 'ACE_ACC5301403'



conn_str_db = f'DRIVER={{SQL Server}};SERVER={ip_sql_server},{port_sql_server};DATABASE={database};UID={username_sql_server};PWD={password_sql_server}'
conn = pyodbc.connect(conn_str_db)
cursor = conn.cursor()
# گرفتن نام همه جداول
cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE'")
tables = cursor.fetchall()


query = f"SELECT * FROM DOCB"
df = pd.read_sql(query, conn)

df.to_excel('prs.xlsx')


