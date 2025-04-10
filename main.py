import os
import d2l_functions
import csv_db
import dotenv
import pandas as pd
from datetime import date
from logger_config import logger
import re
import sys


syllabus_query = f"""
        SELECT 
            ou.OrgUnitId, ou.Name, ou.Code, ou.IsActive, ou.CreatedDate,
            ou.Year, ou.Term, ou.Duration, ou.Section, ou.Department, 
            ou.CourseNumber, ou.SectionType,
            co.Location, co.IsDeleted, co.Recorded,
            oua.AncestorOrgUnitId AS FacultyId,
            f.ProjectId
        FROM OrganizationalUnits ou
        LEFT JOIN ContentObjects co ON ou.OrgUnitId = co.OrgUnitId
        LEFT JOIN OrganizationalUnitAncestors oua ON ou.OrgUnitId = oua.OrgUnitId
        LEFT JOIN Faculty f ON oua.AncestorOrgUnitId = f.FacultyId
        WHERE ou.Year = %s 
        AND ou.Term = %s 
        AND f.ProjectId IS NOT NULL
        AND co.Recorded = 0
        AND co.IsDeleted = 0
        AND co.Location IS NOT NULL
        AND co.Location != '';
    """
all_courses_query = f"""
        SELECT 
            ou.OrgUnitId, ou.Name, ou.Code, ou.IsActive, ou.CreatedDate,
            ou.Year, ou.Term, ou.Duration, ou.Section, ou.Department, 
            ou.CourseNumber, ou.SectionType,
            co.Location, co.IsDeleted, co.Recorded,
            oua.AncestorOrgUnitId AS FacultyId,
            f.ProjectId
        FROM OrganizationalUnits ou
        LEFT JOIN ContentObjects co ON ou.OrgUnitId = co.OrgUnitId
        LEFT JOIN OrganizationalUnitAncestors oua ON ou.OrgUnitId = oua.OrgUnitId
        LEFT JOIN Faculty f ON oua.AncestorOrgUnitId = f.FacultyId
        WHERE ou.Year = %s 
        AND ou.Term = %s 
        AND f.ProjectId IS NOT NULL;
    """

def get_config(mode):
    if mode=='full':
        content_object_plugin_id = os.environ["content_object_plugin_id"]
        org_units_plugin_id = os.environ["org_units_plugin_id"]
        org_units_ancestors_plugin_id = os.environ["org_units_ancestors_plugin_id"]
    else:
        content_object_plugin_id = os.environ["diff_content_object_plugin_id"]
        org_units_plugin_id = os.environ["diff_org_units_plugin_id"]
        org_units_ancestors_plugin_id = os.environ["diff_org_units_ancestors_plugin_id"]
    return {
        "bspace_url": os.environ["bspace_url"],
        "client_id": os.environ["client_id"],
        "client_secret": os.environ["client_secret"],
        "scope": os.environ["scope"],
        "refresh_token": os.environ["refresh_token"],
        "datasets":[
            {"schema_id":os.environ["content_object_schema_id"], "plugin_id":content_object_plugin_id},
            {"schema_id":os.environ["org_units_schema_id"], "plugin_id":org_units_plugin_id},
            {"schema_id":os.environ["org_units_ancestors_schema_id"], "plugin_id":org_units_ancestors_plugin_id}
        ],
        "current_term":os.environ["current_term"]
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

def get_academic_term(current_date):
    year = current_date.year
    if (current_date>date(year,8,24) and current_date<=date(year,12,31)):
        return ([{'term': 'FW', 'year':year, 'identifier':'FW'}])
    elif (current_date>=date(year,1,1) and current_date<date(year,3,27)):
        return ([{'term': 'FW', 'year':year-1, 'identifier':'FW'}])
    else:
        return ([{'term': 'SP', 'year':year, 'identifier':'SP'}, {'term': 'SU', 'year':year, 'identifier':'SPSU'}])

def set_refresh_token(refresh_token):
    os.environ["refresh_token"] = refresh_token
    dotenv.set_key(dotenv_file, "refresh_token", os.environ["refresh_token"])
    dotenv.load_dotenv(dotenv_file)

def set_current_term(term):
    os.environ["current_term"] = term
    dotenv.set_key(dotenv_file, "current_term", os.environ["current_term"])
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

                logger.info(f"Download link for Schema ID {schema_id}, Plugin ID {plugin_id}: {download_link}")
                d2l_functions.save_and_unzip_file(download_link, access_token, datahub_path)

            except (IndexError, KeyError) as e:
                logger.error(f"Error: No valid download link found for Schema ID {schema_id}, Plugin ID {plugin_id}. Details: {e}")
        else:
            logger.error(f"Failed to retrieve extract for Schema ID {schema_id}, Plugin ID {plugin_id}. "
                f"Status code: {create_bds_extract.status_code}")

def download_upload_syllabus(df):
    try:
        # Loop through each row in the CSV
        for index, row in df.iterrows():
            filetype = classify_location(row['Location'])
            download_syllabus(row, filetype)
            upload_syllabus(row, filetype)
    except Exception as e:
        logger.error(f"An error occurred: {e}")

def download_syllabus(row, filetype):
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
        logger.info(f"Processing: {file_url} -> {download_path}")
        
        if (filetype=='d2l'):
            path = os.path.join(download_path, f"syllabus_{orgUnitCode}.html")
            create_blank_syllabus(path)
        elif (filetype=='Link'):
            pass
        else:
            filename = d2l_functions.save_file(file_url, access_token, download_path, orgUnitCode)
                
            if filename:
                logger.info(f"File saved successfully: {filename}")
            else:
                logger.error(f"Failed to save file for {orgUnitId}")
    except Exception as e:
        logger.error(f"An error occurred: {e}")


def create_blank_syllabus(path):
    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Unavailable Syllabus</title>
        <link rel="stylesheet" href="{config['bspace_url']}/shared/Widgets/SyllabusUpload/css/syllabus_collection_styles.css" />
    </head>
    <body>
            <h3>Unavailable Syllabus.</h3>
            <p>The syllabus for this course cannot be retrieved from its current Brightspace location (e.g., Discussions, Assignments, etc). Please contact the instructor to have them upload the syllabus to the Content area of Brightspace instead.</p>
    </body>
    </html>
    """
    with open(path, 'w', encoding='utf-8') as f:
        f.write(html_content)


def upload_syllabus(row, filetype):
    try:
        # Construct the URL with the row's Location value
        orgUnitId = row['ProjectId']
        location = row['Location']
        department = str(row['Department'])
        year = str(row['Year'])
        term = str(row['Term'])

        upload_url = f"{config['bspace_url']}/d2l/api/lp/1.47/{orgUnitId}/managefiles/file/upload"
        #file_name = os.path.basename(location)
        if filetype=='Link':
            pass
        else:
            _, file_extension = os.path.splitext(os.path.basename(location))
            if (filetype=='d2l'): file_extension = '.html'
            file_name = f"syllabus_{row['Code']}{file_extension}"
            file_path = f"{base}/{department}/{year}/{term}/{file_name}"
            file_key = d2l_functions.initiate_resumable_upload(config['bspace_url'], upload_url, access_token, file_path)
            if (file_key):
                save_file_payload = {"fileKey":file_key,
                                    "relativePath": f"{department}/{year}/{term}"}
                d2l_functions.post_with_auth(f"{config['bspace_url']}/d2l/api/lp/1.47/{orgUnitId}/managefiles/file/save?overwriteFile=true", access_token, data=save_file_payload, json_data=False)


    except Exception as e:
        logger.error(f"An error occurred: {e}")


def create_BS_folders(df, year, term):
    
    grouped = df.groupby("Department").agg({
        'ProjectId': 'first'
    }).reset_index()

    for index, row in grouped.iterrows():
        orgUnitId = row['ProjectId']
        department = row['Department']
        create_folder_url = f"{config['bspace_url']}/d2l/api/lp/1.47/{orgUnitId}/managefiles/folder"
        check_folder_url = f"{config['bspace_url']}/d2l/api/lp/1.47/{orgUnitId}/managefiles/"

        if not (d2l_functions.is_folder_exists(check_folder_url, access_token, department)):
            create_folder_payload = {"RelativePath": f"{department}"}
            d2l_functions.post_with_auth(create_folder_url, access_token, data=(create_folder_payload), json_data=True)

        if not (d2l_functions.is_folder_exists(f"{check_folder_url}?path={department}", access_token, str(year))):
            create_folder_payload = {"RelativePath": f"{department}/{year}"}
            d2l_functions.post_with_auth(create_folder_url, access_token, data=(create_folder_payload), json_data=True)

        if not (d2l_functions.is_folder_exists(f"{check_folder_url}?path={department}/{year}", access_token, term)):
            create_folder_payload = {"RelativePath": f"{department}/{year}/{term}"}
            d2l_functions.post_with_auth(create_folder_url, access_token, data=(create_folder_payload), json_data=True)


def check_root_module(data, root_title):
    """Check if a module with the given root_title exists in the root level of Modules.
    Returns ModuleId if found, otherwise None."""
    for module in data.get("Modules", []):
        if module.get("Title") == root_title:
            return module.get("ModuleId")
    return None

def check_child_module(data, root_title, child_title):
    """Check if a child module with child_title exists under the specified root module.
    Returns ModuleId if found, otherwise None."""
    for module in data.get("Modules", []):
        if module.get("Title") == root_title:
            for child in module.get("Modules", []):
                if child.get("Title") == child_title:
                    return child.get("ModuleId")
    return None

def check_topic_in_module(data, root_title, child_title, topic_title):
    """Check if a topic with topic_title exists under the specified root module and child module.
    Returns TopicId if found, otherwise None."""
    for module in data.get("Modules", []):
        if module.get("Title") == root_title:
            for child in module.get("Modules", []):
                if child.get("Title") == child_title:
                    for topic in child.get("Topics", []):
                        if topic.get("Title") == topic_title:
                            return topic.get("TopicId")
    return None


def add_content_module(df, year, term):
    grouped = df.groupby("Department").agg({
        'ProjectId': 'first'
    }).reset_index()

    for index, row in grouped.iterrows():
        orgUnitId = row['ProjectId']
        department = row['Department']
        root_module_payload = {
            "Title": department,
            "ShortTitle": None,
            "Type": 0,
            "ModuleStartDate": None,
            "ModuleEndDate": None,
            "ModuleDueDate": None,
            "IsHidden": False,
            "IsLocked": False,
            "Description": None,
            "Duration": None
        }
        child_module_payload = {
            "Title": year,
            "ShortTitle": None,
            "Type": 0,
            "ModuleStartDate": None,
            "ModuleEndDate": None,
            "ModuleDueDate": None,
            "IsHidden": False,
            "IsLocked": False,
            "Description": None,
            "Duration": None
        }
        topic_payload = {
            "Title": f"Term - {term}",
            "ShortTitle": "",
            "Type": 1,
            "TopicType": 1,
            "Url": f"/content/enforced/{orgUnitId}-Project-{orgUnitId}-PSPT/{department}/{str(year)}/{term}/syllabus_table_{str(department)}_{str(year)}_{str(term)}.html",
            "StartDate": None,
            "EndDate": None,
            "DueDate": None,
            "IsHidden": False,
            "IsLocked": False,
            "OpenAsExternalResource": None,
            "Description": None,
            "MajorUpdate": None,
            "MajorUpdateText": None,
            "ResetCompletionTracking": None,
            "Duration": None
        }

        toc = d2l_functions.get_with_auth(f"{config['bspace_url']}/d2l/api/le/1.80/{orgUnitId}/content/toc", access_token)
        toc_json = toc.json()
        
        # checking if Department module exists, create if not
        root_module_id = check_root_module(toc_json, department)
        if  root_module_id is None:
            root_module_call = d2l_functions.post_with_auth(f"{config['bspace_url']}/d2l/api/le/1.80/{orgUnitId}/content/root/", access_token, data=(root_module_payload), json_data=True)
            root_module_id = root_module_call.json()['Id']
        
        # checking if Year module exists in given Department module, create if not
        child_module_id =  check_child_module(toc_json, department, str(year))
        if child_module_id is None:
            child_module_call = d2l_functions.post_with_auth(f"{config['bspace_url']}/d2l/api/le/1.80/{orgUnitId}/content/modules/{root_module_id}/structure/", access_token, data=(child_module_payload), json_data=True)
            child_module_id = child_module_call.json()['Id']

        # check if topic html file exists in given Department and Year, if not create topic linked to an existing html file in the Course File Management 
        topic_id = check_topic_in_module(toc_json, department, str(year), f"Term - {term}")
        if topic_id is None:
            topic_call = d2l_functions.post_with_auth(f"{config['bspace_url']}/d2l/api/le/1.80/{orgUnitId}/content/modules/{child_module_id}/structure/", access_token, data=(topic_payload), json_data=True)
            


def upload_content_html(df, year, term):
    grouped = df.groupby("Department").agg({
        'ProjectId': 'first'
    }).reset_index()

    for index, row in grouped.iterrows():
        orgUnitId = row['ProjectId']
        department = row['Department']
        upload_url = f"{config['bspace_url']}/d2l/api/lp/1.47/{orgUnitId}/managefiles/file/upload"
        file_name = f"syllabus_table_{str(department)}_{str(year)}_{str(term)}.html"
        file_path = f"{base}/{department}/{year}/{term}/{file_name}"
        file_key = d2l_functions.initiate_resumable_upload(config['bspace_url'], upload_url, access_token, file_path)
        if (file_key):
            save_file_payload = {"fileKey":file_key,
                                 "relativePath": f"{department}/{year}/{term}"}
            d2l_functions.post_with_auth(f"{config['bspace_url']}/d2l/api/lp/1.47/{orgUnitId}/managefiles/file/save?overwriteFile=true", access_token, data=save_file_payload, json_data=False)


def classify_location(value):
    value = str(value).strip()
    if re.match(r'^https?://', value, re.IGNORECASE):
        return 'Link'

    if value.lower().startswith('d2l') or value.startswith('/d2l/'):
        return 'd2l'

    return None

def generate_syllabus_html(df, base_output_dir):
    # Group by Department, Year, and Term
    grouped = df.groupby(["Department", "Year", "Term"])

    # Generate HTML files in each corresponding folder
    for (department, year, term), group in grouped:
        group = group.sort_values(by=["Duration","CourseNumber","Section"], ascending=True)
        # Count total courses and syllabuses recorded
        total_courses = len(group)
        recorded_syllabuses = group['Recorded'].fillna(0).astype(int).sum()
        recorded_percentage = (recorded_syllabuses / total_courses) * 100 if total_courses > 0 else 0

        # Define folder structure
        folder_path = os.path.join(base_output_dir, str(department), str(year), str(term))
        os.makedirs(folder_path, exist_ok=True)

        # Define file path
        file_path = os.path.join(folder_path, f"syllabus_table_{str(department)}_{str(year)}_{str(term)}.html")

        # Create HTML table
        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <script src="https://ajax.googleapis.com/ajax/libs/jquery/3.7.1/jquery.min.js"></script>
            <link rel="stylesheet" href="https://cdn.datatables.net/2.2.2/css/dataTables.dataTables.css" />
            <link rel="stylesheet" href="{config['bspace_url']}/shared/Widgets/SyllabusUpload/css/syllabus_collection_styles.css" />
            <script src="https://cdn.datatables.net/2.2.2/js/dataTables.js"></script>    
            <title>Syllabus Table for {department} - {year} - {term}</title>
        </head>
        <body>
            <h2>Syllabus for {department} - {year} - {term}</h2>
            <p>Total Courses: {total_courses}, Syllabuses Available: {recorded_syllabuses} ({recorded_percentage:.2f}%)</p>
            <table id="{department}-{year}-{term}" class="display">
                <thead>
                    <tr>
                        <th>Course Code</th>
                        <th>Action</th>
                    </tr>
                </thead>
                <tbody>
        """

        # Add table rows
        for _, row in group.iterrows():

            # Handle NaN values in Recorded
            row['Recorded'] = 0 if pd.isna(row['Recorded']) else int(row['Recorded'])

            if row['Recorded']==1:
                if (classify_location(row['Location']) == 'Link'):
                    syllabus_link = f"<a href={row['Location']} target='_blank'>{row['Code']}</a>"
                else:
                    _, file_extension = os.path.splitext(os.path.basename(row['Location']))
                    if (classify_location(row['Location']) == 'd2l'): file_extension = '.html'
                    href = f"/content/enforced/{row['ProjectId']}-Project-{row['ProjectId']}-PSPT/{row['Department']}/{row['Year']}/{row['Term']}/syllabus_{row['Code']}{file_extension}"
                    syllabus_link = f"<a href={href} target='_blank'>{row['Code']}</a>"
            else: 
                syllabus_link = row['Code']

            html_content += f"""
                <tr>
                    <td>{syllabus_link}</td>
                    <td></td>
                </tr>
            """

        # Close HTML tags
        html_content += f"""
                </tbody>
            </table>
            <script>$('#{department}-{year}-{term}').DataTable({{
                lengthMenu: [
                    [50, 100, 150, 200, 250],
                    ['50 per page', '100 per page', '150 per page', '200 per page', '250 per page']
                    ],
                language: {{
                    lengthMenu: '_MENU_',
                    searchPlaceholder: 'Search For ...',
                    search: '_INPUT_'
                }},
                stateSave: true,
                info: false
            }});
            </script>
        </body>
        </html>
        """

        # Write to file
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(html_content)

# ******** main.py ********

# get configs
logger.info("Started...")

if len(sys.argv) != 2:
    print("Usage: python run_mode.py [full|differential]")
    logger.error("Terminating, incorrect run. Usage: python3 main.py [full|differential]")
    sys.exit(1)

mode = sys.argv[1].lower()

if mode not in ['full', 'differential']:
    print("Error: Invalid argument. Only 'full' or 'differential' are allowed.")
    logger.error("Terminating: Invalid argument. Only 'full' or 'differential' are allowed.")
    sys.exit(1)
    
dotenv_file = dotenv.find_dotenv()
dotenv.load_dotenv(dotenv_file)
config = get_config(mode)
base = 'downloads'
os.makedirs(base, exist_ok=True)

# Get access token and update the refresh token in environment variables
authorize_to_d2l = d2l_functions.trade_in_refresh_token(config)
access_token = authorize_to_d2l['access_token']
refresh_token = authorize_to_d2l['refresh_token']
set_refresh_token(refresh_token)
logger.info('Tokens are set.')


# Download and extract data hub reports
datahub_path = 'datahub/'
os.makedirs(datahub_path, exist_ok=True)
logger.info('Downloading reports.')
#get_data_hub_reports()
logger.info('Reports are downloaded.')


# Get database configuration
db_config = get_db_config()
logger.info('Pushing reports into Database')
csv_db.setDb(db_config)
logger.info('Database updated.')

today = date.today()
term_year = get_academic_term(today)

logger.info('Current term identified.')
for each in term_year:
    year = each['year']
    term = each['term']

    #create folders in the Brightspace
    logger.info(f'Request for all course data initiated for given term: {term} and year: {year}.')
    all_courses = csv_db.get_sylabus(db_config, all_courses_query, term, year)

    logger.info('Creating folders in the BS')
    create_BS_folders(all_courses, year, term)

    logger.info('Generating folders in the server and html per Department->Year->Term.')
    generate_syllabus_html(all_courses,base)
    
    logger.info('Uploading html files into Course Management area before creating modules.')
    upload_content_html(all_courses, year, term)

    logger.info('Checking if Content Modules and Topics exists for given Departments->Years-Terms')
    add_content_module(all_courses, year, term)

    # Upload todays Sylabusses
    logger.info('Requesting syllabus data that are not been pushed to BS for given year and term.')
    syllabus_to_run = csv_db.get_sylabus(db_config, syllabus_query, term, year)
    logger.info('Downloading syllabuses and uploading them into Project sites.')
    #download_upload_syllabus(syllabus_to_run)

    logger.info('Updating Recorded field in DB.')
    csv_db.update_syllabus_recorded(db_config, syllabus_to_run)

    logger.info('Requesting new all courses data for given term and year.')
    all_courses = csv_db.get_sylabus(db_config, all_courses_query,  term, year)
    logger.info('Generating folders and html files in the server again to update the html files with new records.')
    generate_syllabus_html(all_courses, base)

    logger.info('Uploading updated html files to BS')
    upload_content_html(all_courses, year, term)

logger.info('End.')

