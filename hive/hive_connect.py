from pyhive import hive

hostname = "localhost"
port = "10000"
database = "default"


def hiveconnection(host, port, database):
    conn = hive.Connection(host=host, port=port, auth="NOSASL")
    cur = conn.cursor()
    cur.execute("SHOW DATABASES")
    result = cur.fetchall()

    return result


output = hiveconnection(hostname, port, database)
print(output)
