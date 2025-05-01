import pandas as pd
import mysql.connector
import os
import re
from logger_config import logger
import dotenv
import os

department_courses_query = f"""
        SELECT 
            ou.OrgUnitId, ou.Name, ou.Code, ou.IsActive, ou.CreatedDate,
            ou.Year, ou.Term, ou.Duration, ou.Section, ou.Department, 
            ou.CourseNumber, ou.SectionType, ou.Recorded,
            co.Location, co.IsDeleted,
            oua.AncestorOrgUnitId AS FacultyId,
            f.ProjectId
        FROM OrganizationalUnits ou
        LEFT JOIN ContentObjects co ON ou.OrgUnitId = co.OrgUnitId
        LEFT JOIN OrganizationalUnitAncestors oua ON ou.OrgUnitId = oua.OrgUnitId
        LEFT JOIN Faculty f ON oua.AncestorOrgUnitId = f.FacultyId
        WHERE ou.Year = %s 
        AND ou.Term = %s 
        AND ou.Department = %s
        AND f.ProjectId IS NOT NULL;
    """

file_path = 'datahub/'
os.makedirs(file_path, exist_ok=True)

def get_db_config():
    return {
        "host": os.environ["host"],
        "user": os.environ["user"],
        "password": os.environ["password"],
        "database": os.environ["database"], 
    }

dotenv_file = dotenv.find_dotenv()
dotenv.load_dotenv(dotenv_file)

db_config = get_db_config()

# Connect to the database
def get_db_connection():
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
    
    filtered_df = filtered_df.copy()
    if not filtered_df.empty:
        filtered_df.loc[:, 'Recorded'] = 0
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
def setDb():
    conn = get_db_connection()

    logger.info('Running OrganizationalUnits and Content Objects...')
    setOrganizationalUnits(conn)
    logger.info('OrganizationalUnits and ContentObject tables updated successfully.')

    logger.info('Running OrganizationalUnitAncestors...')
    setAncestors(conn)
    logger.info('OrganizationalUnitAncestors table updated successfully.')

    update_ancestor_orgunit_id_for_btgd(conn)

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


def get_sylabus(query, term, year):

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        logger.info("Executing syllabus query...")
        cursor.execute(query, (year, term))
        syllabus_results = cursor.fetchall()

        # Fetch column names
        column_names = [desc[0] for desc in cursor.description]
        df_syllabus = pd.DataFrame(syllabus_results, columns=column_names)

        if df_syllabus.empty:
            logger.warning("No syllabus records found.")

        return df_syllabus

    except mysql.connector.Error as err:
        logger.error(f"Error executing syllabus query: {err}")
        return pd.DataFrame()  # Return an empty DataFrame instead of None

    finally:
        cursor.close()
        conn.close()


def update_syllabus_recorded(df, value=1):
    batch_size=1000
    conn = get_db_connection()
    cursor = conn.cursor()

    update_query = """
        UPDATE OrganizationalUnits 
        SET Recorded = %s 
        WHERE OrgUnitId = %s;
    """
    
    # Prepare the data as a list of tuples
    data = [(int(value), int(row['OrgUnitId'])) for _, row in df.iterrows()]
    try:
        for i in range(0, len(data), batch_size):
            batch = data[i:i + batch_size]
            cursor.executemany(update_query, batch)
            conn.commit()

    except mysql.connector.Error as err:
        logger.error(f"Error updating OrganizationalUnits with Recorded fields value: {err}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()


def upsert_content_object(content_object_id, org_unit_id, title, content_type, location, last_modified, is_deleted):
    conn = get_db_connection()
    cursor = conn.cursor()
    query = """
        INSERT INTO ContentObjects (
            ContentObjectId, OrgUnitId, Title, ContentObjectType, Location, LastModified, IsDeleted
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE 
            Title = VALUES(Title),
            ContentObjectType = VALUES(ContentObjectType),
            Location = VALUES(Location),
            LastModified = VALUES(LastModified),
            IsDeleted = VALUES(IsDeleted);
    """
    values = (
        content_object_id,
        org_unit_id,
        title,
        content_type,
        location,
        last_modified,
        is_deleted
    )
    try:
        cursor.execute(query, values)
        conn.commit()
        logger.info(f"Upserted ContentObject for OrgUnitId={org_unit_id}")
    except mysql.connector.Error as err:
        logger.error(f"Error upserting ContentObject: {err}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()            


def get_orgUnitId_by_code(code):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        query = "SELECT OrgUnitId FROM OrganizationalUnits WHERE Code = %s"
        cursor.execute(query, (code,))
        result = cursor.fetchone()
        return result[0] if result else None

    except mysql.connector.Error as err:
        print(f"Database error: {err}")
        return None

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def get_department_cources(term, year, department):

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        logger.info("Executing department courses query...")
        cursor.execute(department_courses_query, (year, term, department))
        syllabus_results = cursor.fetchall()

        # Fetch column names
        column_names = [desc[0] for desc in cursor.description]
        df_syllabus = pd.DataFrame(syllabus_results, columns=column_names)

        if df_syllabus.empty:
            logger.warning("No course records found.")

        return df_syllabus

    except mysql.connector.Error as err:
        logger.error(f"Error executing syllabus query: {err}")
        return pd.DataFrame()  # Return an empty DataFrame instead of None

    finally:
        cursor.close()
        conn.close()


def update_btgd_ancestor_orgunit(conn):
    cursor = conn.cursor()
    try:
        # Get OrgUnitIds for BTGD department that are not already mapped to AncestorOrgUnitId = 6937
        select_query = """
            SELECT ou.OrgUnitId 
            FROM OrganizationalUnits ou
            WHERE ou.Department = 'BTGD'
            AND ou.OrgUnitId NOT IN (
                SELECT OrgUnitId 
                FROM OrganizationalUnitAncestors 
                WHERE AncestorOrgUnitId = 6937
            );
        """
        cursor.execute(select_query)
        orgunit_ids = cursor.fetchall()
        
        # Remove any existing record that would conflict
        delete_query = """
            DELETE FROM OrganizationalUnitAncestors
            WHERE OrgUnitId IN (
                SELECT OrgUnitId FROM OrganizationalUnits WHERE Department = 'BTGD'
            );
        """ 
        cursor.execute(delete_query)

        for (orgunit_id,) in orgunit_ids:
            # Insert the new mapping
            insert_query = """
                INSERT INTO OrganizationalUnitAncestors (OrgUnitId, AncestorOrgUnitId)
                VALUES (%s, 6937);
            """
            cursor.execute(insert_query, (orgunit_id,))

        conn.commit()
        logger.info("BTGD ancestor OrgUnit updates applied successfully.")
    except mysql.connector.Error as err:
        logger.error(f"Database error: {err}")
        if conn:
            conn.rollback()
    finally:
        if cursor:
            cursor.close()