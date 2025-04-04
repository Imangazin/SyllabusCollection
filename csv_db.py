import pandas as pd
import mysql.connector
import os
import re
from logger_config import logger

file_path = 'datahub/'
os.makedirs(file_path, exist_ok=True)

# Connect to the database
def get_db_connection(db_config):
    return mysql.connector.connect(**db_config)

# Read CSV file
def readCSV(file_path):
    if os.path.exists(file_path):
        return pd.read_csv(file_path, low_memory=False)
    else:
        return pd.DataFrame()

# Function to split Code into 7 components and validate the format
def split_code(code):
    if pd.isna(code):  # Check for NaN values
        return [None] * 7
    
    # Regular expression to validate the first 15 characters
    valid_format = re.match(r'^\d{4}-[A-Z]{2}-D\d{2}-S\d{2}', str(code)[:15])
    if not valid_format:  # If format is invalid
        return [None] * 7

    components = str(code).split('-')  # Convert to string and split by '-'
    if len(components) == 7:  # Only process if there are exactly 7 components
        return components
    return [None] * 7  # Placeholder if the format doesn't match

# Convert datetime columns to MySQL-friendly format
def convert_datetime_columns(df, datetime_columns):
    df = df.copy()  # Ensure a deep copy to avoid modifying a slice
    for col in datetime_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")  # Convert to datetime format
            
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                df[col] = df[col].dt.strftime("%Y-%m-%d %H:%M:%S")  # Format for MySQL
            else:
                logger.warning(f"Warning: Column {col} contains non-datetime values. Check for invalid formats.")
    return df


# Retrieve column names ordered as the ordinal position and their data types from a given table
def get_table_columns(cursor, table_name):
    """Retrieve column names and their data types in the correct table order."""
    query = f"""
    SELECT COLUMN_NAME, DATA_TYPE
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = '{table_name}'
    ORDER BY ORDINAL_POSITION;
    """
    cursor.execute(query)
    return {row[0]: row[1] for row in cursor.fetchall()}


def setOrganizationalUnits(conn):

    cursor = conn.cursor()

    organizational_units_df = readCSV(f'{file_path}/OrganizationalUnits.csv')
    
    # Filter out Course Offerings only
    organizational_units_df[organizational_units_df['OrgUnitTypeId'] == 3]

    # Select only the needed columns from OrganizationalUnits.csv
    organizational_units_filtered = organizational_units_df[[
        'OrgUnitId', 'Name', 'Code', 'IsActive', 'CreatedDate', 'IsDeleted'
    ]]

    # Extract the split components of Code into separate columns
    split_columns = organizational_units_filtered['Code'].apply(split_code).apply(pd.Series)
    split_columns.columns = ['Year', 'Term', 'Duration', 'Section', 'Department', 'CourseNumber', 'SectionType']

    # Safely add the new columns to the DataFrame
    organizational_units_filtered = pd.concat([organizational_units_filtered, split_columns], axis=1)

    # Filter out records based on blank values and deletion flags 
    filtered_df = organizational_units_filtered[
        (organizational_units_filtered['Year'].notna()) &
        (organizational_units_filtered['Department'].notna()) &
        (organizational_units_filtered['Term'].notna()) & 
        (~organizational_units_filtered['IsDeleted'].astype(bool))    # Ensures IsDeleted is False or 0
    ]
    
    table_columns_dict = get_table_columns(cursor, 'OrganizationalUnits')
    table_columns = list(table_columns_dict.keys())
    datetime_columns = [col for col, dtype in table_columns_dict.items() if dtype in ("datetime", "timestamp")]
    
    filtered_df = convert_datetime_columns(filtered_df, datetime_columns)
    filtered_df = filtered_df.astype(object).where(pd.notnull(filtered_df), None)

    write_to_table(conn, 'OrganizationalUnits', filtered_df, table_columns)

    # Running ContentObjects
    content_objects_df = getContentObjects()
    filtered_content_objects_df = content_objects_df[
        content_objects_df["OrgUnitId"].isin(filtered_df["OrgUnitId"])
    ]
    table_columns_dict = get_table_columns(cursor, 'ContentObjects')
    table_columns = list(table_columns_dict.keys())
    datetime_columns = [col for col, dtype in table_columns_dict.items() if dtype in ("datetime", "timestamp")]

    filtered_content_objects_df = filtered_content_objects_df.copy()  # Ensure it's a copy
    if not filtered_content_objects_df.empty:
        filtered_content_objects_df.loc[:, 'Recorded'] = 0
        filtered_content_objects_df = convert_datetime_columns(filtered_content_objects_df, datetime_columns)
        filtered_content_objects_df = filtered_content_objects_df.astype(object).where(pd.notnull(filtered_content_objects_df), None)
        write_to_table(conn, 'ContentObjects', filtered_content_objects_df, table_columns)

    cursor.close()


def getContentObjects():

    content_objects_df = readCSV(f'{file_path}/ContentObjects.csv')

    # Select only the needed columns from OrganizationalUnits.csv
    content_objects_filtered = content_objects_df[[
        'ContentObjectId', 'OrgUnitId', 'Title', 'ContentObjectType', 'Location', 'LastModified', 'IsDeleted'
    ]]

    # Scanning for 'syllabus' or 'course outline' in the ContentObjects for Topic type only
    filtered_content_objects = content_objects_filtered[
        (content_objects_filtered['Title'].str.contains('Syllabus|course outline', case=False, na=False)) &
        (content_objects_filtered['ContentObjectType'] == 'Topic')
    ]

    filtered_content_objects = filtered_content_objects.copy()  # Create a deep copy to ensure no warnings
    filtered_content_objects.loc[:, 'LastModified'] = pd.to_datetime(filtered_content_objects['LastModified'], errors='coerce')

    # Group by OrgUnitId and keep the row with the latest LastModified
    filtered_df = filtered_content_objects.loc[
        filtered_content_objects.groupby('OrgUnitId')['LastModified'].idxmax()
    ].reset_index(drop=True)

    return filtered_df



def setAncestors(conn):
    
    cursor = conn.cursor()

    ancestors_df = readCSV(f'{file_path}/OrganizationalUnitAncestors.csv')
    ancestors_table_columns_dict = get_table_columns(cursor, 'OrganizationalUnitAncestors')
    ancestors_table_columns = list(ancestors_table_columns_dict.keys())
    
    write_to_table(conn, 'OrganizationalUnitAncestors', ancestors_df, ancestors_table_columns)
    cursor.close()

# Sets the temporarly tables and writes daily data to them
def setDb(db_config):
    conn = get_db_connection(db_config)

    logger.info('Running OrganizationalUnits and Content Objects...')
    setOrganizationalUnits(conn)
    logger.info('OrganizationalUnits and ContentObject tables updated successfully.')

    logger.info('Running OrganizationalUnitAncestors...')
    setAncestors(conn)
    logger.info('OrganizationalUnitAncestors table updated successfully.')

    conn.close()


def write_to_table(conn, table, df, table_columns, batch_size=1000):
    cursor = conn.cursor()

    placeholders = ", ".join(["%s"] * len(table_columns))
    #update_placeholders = ", ".join([f"{col} = VALUES({col})" for col in table_columns])
    update_placeholders = ", ".join([f"{col} = VALUES({col})" for col in table_columns if col != 'Recorded'])

    query = f"""
        INSERT INTO {table} ({', '.join(table_columns)}) 
        VALUES ({placeholders}) 
        ON DUPLICATE KEY UPDATE {update_placeholders};
    """

    data = [tuple(row) for _, row in df[table_columns].iterrows()]
    if not data:
        logger.info(f"Skipping '{table}' as there are no records to insert.")
        cursor.close()
        return
        
    try:
        for i in range(0, len(data), batch_size):
            batch = data[i:i + batch_size]
            cursor.executemany(query, batch)
            conn.commit()
    except mysql.connector.Error as err:
        logger.error(f"Error inserting into '{table}': {err}")
    finally:
        cursor.close()


def get_sylabus(db_config, query, term, year):

    conn = get_db_connection(db_config)
    cursor = conn.cursor()

    try:
        logger.info("Executing syllabus query...")
        cursor.execute(query, (year, term))
        syllabus_results = cursor.fetchall()

        # Fetch column names
        column_names = [desc[0] for desc in cursor.description]
        df_syllabus = pd.DataFrame(syllabus_results, columns=column_names)

        # Save the results as a CSV file DEBUGing purposes, delete this line later
        syllabus_csv_file = os.path.join(file_path, "syllabus_data.csv")
        df_syllabus.to_csv(syllabus_csv_file, index=False)
        logger.info(f"Syllabus query results saved to: {syllabus_csv_file}")

        if df_syllabus.empty:
            logger.warning("No syllabus records found.")

        return df_syllabus

    except mysql.connector.Error as err:
        logger.error(f"Error executing syllabus query: {err}")
        return pd.DataFrame()  # Return an empty DataFrame instead of None

    finally:
        cursor.close()
        conn.close()


def update_syllabus_recorded(db_config, df, batch_size=1000):
    conn = get_db_connection(db_config)
    cursor = conn.cursor()
    
    update_query = """
        UPDATE ContentObjects 
        SET Recorded = %s 
        WHERE OrgUnitId = %s;
    """
    
    # Prepare the data as a list of tuples
    data = [(1, row['OrgUnitId']) for _, row in df.iterrows()]

    try:
        for i in range(0, len(data), batch_size):
            batch = data[i:i + batch_size]
            cursor.executemany(update_query, batch)
            conn.commit()

    except mysql.connector.Error as err:
        logger.error(f"Error updating ContentObjects: {err}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()


# Function to execute SQL script from file
def create_main_tables(file_path, db_config):
    try:
        # Connect to the database
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()

        # Read SQL file
        with open(file_path, "r", encoding="utf-8") as sql_file:
            sql_script = sql_file.read()

        # Split SQL statements and execute them one by one
        for statement in sql_script.split(";"):
            if statement.strip():  # Ignore empty statements
                cursor.execute(statement)

        # Commit changes and close connection
        connection.commit()
        logger.info("Tables created successfully.")
    
    except mysql.connector.Error as err:
        logger.error(f"Error: {err}")

    finally:
        cursor.close()
        connection.close()
