import os
import d2l_functions
import csv_db
import dotenv
import pandas as pd
import json

def get_config():
    return {
        "bspace_url": os.environ["bspace_url"],
        "client_id": os.environ["client_id"],
        "client_secret": os.environ["client_secret"],
        "scope": os.environ["scope"],
        "refresh_token": os.environ["refresh_token"],
         "datasets":[
            {"schema_id":os.environ["content_object_schema_id"], "plugin_id":os.environ["content_object_plugin_id"]},
            {"schema_id":os.environ["org_units_schema_id"], "plugin_id":os.environ["org_units_plugin_id"]},
            {"schema_id":os.environ["org_units_ancestors_schema_id"], "plugin_id":os.environ["org_units_ancestors_plugin_id"]}
        ]
    }

def get_db_config():
    return {
        "host": os.environ["host"],
        "user": os.environ["user"],
        "password": os.environ["password"],
        "database": os.environ["database"], 
    }

def get_table_names():
    return os.environ["table_name"].split(',')

def set_refresh_token(refresh_token):
    os.environ["refresh_token"] = refresh_token
    dotenv.set_key(dotenv_file, "refresh_token", os.environ["refresh_token"])
    dotenv.load_dotenv(dotenv_file)

def get_data_hub_reports():
    # Loop through datasets in config
    for dataset in config['datasets']:
        
        schema_id = dataset["schema_id"]
        plugin_id = dataset["plugin_id"]

        # Define file paths
        #download_path = os.path.join(datahub_path, f'{plugin_id}.zip')
        #extract_to = datahub_path

        # Extract download link
        bds_extract_link = f"{config['bspace_url']}/d2l/api/lp/1.47/datasets/bds/{schema_id}/plugins/{plugin_id}/extracts"
        create_bds_extract = d2l_functions.get_with_auth(bds_extract_link, access_token)

        # Check if the request was successful
        if create_bds_extract.status_code == 200:
            try:
                download_link = create_bds_extract.json().get('Objects', [{}])[0].get('DownloadLink')
                if not download_link:
                    raise KeyError("DownloadLink missing in response.")

                print(f"Download link for Schema ID {schema_id}, Plugin ID {plugin_id}: {download_link}")
                d2l_functions.save_and_unzip_file(download_link, access_token, datahub_path)

            except (IndexError, KeyError) as e:
                print(f"Error: No valid download link found for Schema ID {schema_id}, Plugin ID {plugin_id}. Details: {e}")
        else:
            print(f"Failed to retrieve extract for Schema ID {schema_id}, Plugin ID {plugin_id}. "
                f"Status code: {create_bds_extract.status_code}")

def download_upload_syllabus(df):
    try:
        # Loop through each row in the CSV
        for index, row in df.iterrows():
            download_syllabus(row)
            upload_syllabus(row)
    except Exception as e:
        print(f"An error occurred: {e}")

def download_syllabus(row):
    try:
        # Construct the URL with the row's Location value
        orgUnitId = row['OrgUnitId']
        orgUnitCode = row['Code']
        location = row['Location']
        file_url = f"{config['bspace_url']}/d2l/api/lp/1.47/{orgUnitId}/managefiles/file?path={location}"
            
        # Construct the download path using Year, Department, and Department+CourseName
        year = str(row['Year'])
        department = str(row['Department'])
        term = str(row['Term'])
        #course_name = str(row['Department']) + str(row['CourseNumber'])
        download_path = os.path.join(base, department, year, term)
        os.makedirs(download_path, exist_ok=True)  # Ensure the directory exists
            
        # Call save_file function to download and save the file
        print(f"Processing: {file_url} -> {download_path}")
        filename = d2l_functions.save_file(file_url, access_token, download_path, orgUnitCode)
            
        if filename:
            print(f"File saved successfully: {filename}")
        else:
            print(f"Failed to save file for {orgUnitId}")
    except Exception as e:
        print(f"An error occurred: {e}")


def upload_syllabus(row):
    try:
        # Construct the URL with the row's Location value
        orgUnitId = row['ProjectId']
        location = row['Location']
        department = str(row['Department'])
        year = str(row['Year'])
        term = str(row['Term'])
        #course_name = str(row['Department']) + str(row['CourseNumber'])
        create_folder_url = f"{config['bspace_url']}/d2l/api/lp/1.47/{orgUnitId}/managefiles/folder"

        create_folder_payload = {"RelativePath": f"{department}"}
        create_folder = d2l_functions.post_with_auth(create_folder_url, access_token, data=(create_folder_payload), json_data=True)

        create_folder_payload = {"RelativePath": f"{department}/{year}"}
        create_folder = d2l_functions.post_with_auth(create_folder_url, access_token, data=(create_folder_payload), json_data=True)

        create_folder_payload = {"RelativePath": f"{department}/{year}/{term}"}
        create_folder = d2l_functions.post_with_auth(create_folder_url, access_token, data=(create_folder_payload), json_data=True)

        # create_folder_payload = {"RelativePath": f"{department}/{year}/{term}/{course_name}"}
        # create_folder = d2l_functions.post_with_auth(create_folder_url, access_token, data=(create_folder_payload), json_data=True)

        upload_url = f"{config['bspace_url']}/d2l/api/lp/1.47/{orgUnitId}/managefiles/file/upload"
        #file_name = os.path.basename(location)
        _, file_extension = os.path.splitext(os.path.basename(location))
        file_name = f"syllabus_{row['Code']}{file_extension}"
        file_path = f"{base}/{department}/{year}/{term}/{file_name}"
        file_key = d2l_functions.initiate_resumable_upload(config['bspace_url'],upload_url, access_token, file_path)
        if (file_key):
            save_file_payload = {"fileKey":file_key,
                                 "relativePath": f"{department}/{year}/{term}"}
            d2l_functions.post_with_auth(f"{config['bspace_url']}/d2l/api/lp/1.47/{orgUnitId}/managefiles/file/save?overwriteFile=true", access_token, data=save_file_payload, json_data=False)


    except Exception as e:
        print(f"An error occurred: {e}")


def ensure_html_file(department_path, department, year, term):
    """Ensures an HTML file exists in the department directory and initializes it if necessary."""
    html_file_path = os.path.join(department_path, "Sylabuses.html")

    # Create the HTML file if it doesn't exist
    if not os.path.exists(html_file_path):
        with open(html_file_path, "w", encoding="utf-8") as f:
            f.write(f"<html>\n<head><title>{department} Syllabus - {year}</title></head>\n<body>\n<h1>{department} Syllabuses : {term}-{year}</h1>\n<ul>\n</ul>\n</body>\n</html>")

    return html_file_path


def append_to_html_file(html_file_path, file_name, relative_path, orgUnitId):
    """Appends a link to the syllabus file in the HTML file."""
    print("append called")
    new_link = f'<li><a href="/content/enforced/{orgUnitId}-Project-{orgUnitId}-PSPT/{relative_path}?isCourseFile=true" target="_blank">{file_name}</a></li>\n'
    with open(html_file_path, "r+", encoding="utf-8") as f:
        content = f.read()

        # Check if the link already exists
        
        # Move cursor to the end of the <ul> section and insert the new link
        updated_content = content.replace("</ul>", f"{new_link}</ul>")
        f.seek(0)
        f.write(updated_content)
        f.truncate()  # Remove any remaining old content



# ******** main.py ********

# get configs
dotenv_file = dotenv.find_dotenv()
dotenv.load_dotenv(dotenv_file)
config = get_config()

# Get access token and update the refresh token in environment variables
authorize_to_d2l = d2l_functions.trade_in_refresh_token(config)
access_token = authorize_to_d2l['access_token']
refresh_token = authorize_to_d2l['refresh_token']
set_refresh_token(refresh_token)



# Download and extract data hub reports
datahub_path = 'datahub/'
os.makedirs(datahub_path, exist_ok=True)

get_data_hub_reports()



# Get database configuration
db_config = get_db_config()

#csv_db.setDb(db_config)

all_org_units = csv_db.get_sylabus(db_config)

new_syllabuses = all_org_units[all_org_units['Recorded']==0]


test_syllabus_df = new_syllabuses.head(50)

base = 'downloads'
os.makedirs(base, exist_ok=True)

download_upload_syllabus(test_syllabus_df)


# create tables
# csv_db.create_main_tables('tables/tables.sql', db_config)
