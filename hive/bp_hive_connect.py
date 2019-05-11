from impala.dbapi import connect

hostname = "0.0.0.0"
port = "10000"
database = "default"

conn = connect(host=hostname, port=10000, database=database)
cursor = conn.cursor()
cursor.execute('SELECT * from test')
results = cursor.fetchall()
print(results)
