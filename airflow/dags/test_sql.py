import mysql.connector

connection = mysql.connector.connect(
    host="localhost",       # Database host (use "localhost" or the IP of your database)
    user="admin",   # Your MySQL username
    password="1234",  # Your MySQL password
    database="health_metrics"  # Your MySQL database name
)

cursor = connection.cursor()

cursor.execute("SHOW TABLES")

print(cursor.fetchall())

cursor.close()
connection.close()
