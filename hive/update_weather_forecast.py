from impala.dbapi import connect

hostname = "0.0.0.0"
port = "10000"
database = "default"

statement = '''
INSERT INTO test
VALUES (3,'foo2'),
       (4,'bar2')
'''
conn = connect(host=hostname, port=10000, database=database)
cursor = conn.cursor()
cursor.execute(statement)
results = cursor.fetchall()
print(results)