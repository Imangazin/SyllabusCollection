import pandas as pd
import mysql.connector
import os
import dotenv

def get_db_config():
    return {
        "host": os.environ["host"],
        "user": os.environ["user"],
        "password": os.environ["password"],
        "database": os.environ["database"], 
    }

def get_db_connection():
    return mysql.connector.connect(**db_config)

# Load CSV into pandas
df = pd.read_csv("datahub/booklist.csv")

dotenv_file = dotenv.find_dotenv()
dotenv.load_dotenv(dotenv_file)

db_config = get_db_config()

# Connect to MySQL
conn = get_db_connection()
cursor = conn.cursor()

# Insert row by row
for _, row in df.iterrows():
    cursor.execute("""
        INSERT INTO BookList (Term, Department, CourseNumber, Section, LastName, FirstName, AdaptionStatus, Code)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, tuple(row))

conn.commit()
cursor.close()
conn.close()