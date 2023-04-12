import mysql.connector as mysql

def ConnectToDb():
    HOST = "raster.mysql.database.azure.com" # or "domain.com"
    # database name, if you want just to connect to MySQL server, leave it empty
    DATABASE = "FiData"
    # this is the user you create
    USER = "django"
    # user password
    PASSWORD = "Sylvina123!"
    # connect to MySQL server
    db_connection = mysql.connect(host=HOST, database=DATABASE, user=USER, password=PASSWORD)
    print("Connected to:", db_connection.get_server_info())

    return db_connection

