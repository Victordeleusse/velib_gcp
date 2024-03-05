# import findspark
import sqlite3
import requests
# from pyspark.sql import SparkSession, SQLContext
# from pyspark import SparkContext

# findspark.init()
# sc = SparkContext("local")
# sql_c = SQLContext(sc)

def import_data():
    for i in range(1, 3):
        url = f"https://velib.nocle.fr/dump/2024-03-0{i}-data.db"
        response = requests.get(url)
        filename = f"2024-03-0{i}-data.db"
        
        with open(filename, 'wb') as file:
            file.write(response.content)

    for i in range(1, 3):
        filename = f"2024-03-0{i}-data.db"
        print(f"Tables in {filename}:")
        conn = sqlite3.connect(filename)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        for table_name in tables:
            print(table_name[0]) 
        conn.close()

if __name__ == "__main__":
    import_data()


# for i in range(1, 3):
#     filename = f"2024-03-0{i}-data.db"
    
#     conn = sqlite3.connect(filename)
#     cursor = conn.cursor()
    
#     cursor.execute("SELECT * FROM ma_table")  # Remplacez 'ma_table' par le nom de votre table
#     rows = cursor.fetchall()
    
#     print(rows)
    
#     conn.close()