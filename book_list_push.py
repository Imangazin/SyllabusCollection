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

# Load .env before reading env vars
dotenv_file = dotenv.find_dotenv()
dotenv.load_dotenv(dotenv_file)

db_config = get_db_config()

# Ensure pandas NA values become Python None for MySQL
df = df.astype(object).where(pd.notnull(df), None)

# Keep only expected columns in the correct order
cols = [
    'Term', 'Department', 'CourseNumber', 'Section',
    'LastName', 'FirstName', 'AdaptionStatus', 'Code'
]
missing = [c for c in cols if c not in df.columns]
if missing:
    raise ValueError(f"CSV is missing required columns: {missing}")

data = [tuple(row[c] for c in cols) for _, row in df.iterrows()]

# Connect to MySQL and insert
conn = get_db_connection()
cursor = conn.cursor()
try:
    cursor.executemany(
        """
        INSERT INTO BookList (Term, Department, CourseNumber, Section, LastName, FirstName, AdaptionStatus, Code)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """,
        data
    )
    conn.commit()
finally:
    cursor.close()
    conn.close()