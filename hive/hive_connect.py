from pyhive import hive

hostname = "localhost"
port = "10000"
user = "hive"
password = "hive-password"
database = "default"

def hiveconnection(host, port, user, password, database):
    conn = hive.Connection(host=host, port=port, username=user, password=password,
                           database=database, auth="None")
    cur = conn.cursor()
    cur.execute("SELECT * FROM test")
    result = cur.fetchall()

    return result


output = hiveconnection(hostname, port, user, password, database)
print(output)
