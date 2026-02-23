import os
import d2l_functions
import csv_db
import dotenv
import pandas as pd
from datetime import date
from logger_config import logger
import sys
import time


syllabus_query = f"""
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
        AND f.ProjectId IS NOT NULL
        AND ou.Recorded = 0
        AND co.IsDeleted = 0
        AND co.Location IS NOT NULL
        AND co.Location != '';
    """
all_courses_query = f"""
        SELECT 
            ou.OrgUnitId, ou.Name, ou.Code, ou.IsActive, ou.CreatedDate,
            ou.Year, ou.Term, ou.Duration, ou.Section, ou.Department, 
            ou.CourseNumber, ou.SectionType, ou.Recorded,
            co.Location, co.IsDeleted,
            oua.AncestorOrgUnitId AS FacultyId,
            f.ProjectId,
            bl.AdoptionStatus
        FROM OrganizationalUnits ou
        LEFT JOIN ContentObjects co ON ou.OrgUnitId = co.OrgUnitId
        LEFT JOIN OrganizationalUnitAncestors oua ON ou.OrgUnitId = oua.OrgUnitId
        LEFT JOIN Faculty f ON oua.AncestorOrgUnitId = f.FacultyId
        LEFT JOIN (
                SELECT
                    Code,
                    CASE
                    WHEN SUM(AdoptionStatus = 'Complete') > 0 THEN 'Complete'
                    ELSE MAX(AdoptionStatus)
                    END AS AdoptionStatus
                FROM BookList
                GROUP BY Code
                ) bl ON ou.Code = bl.Code
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
        "current_term":os.environ["current_term"],
        "secret_key":os.environ["secret_key"]
    }

def get_academic_term(current_date):
    #return ([{'term': 'FW', 'year':2024, 'identifier':'FW'}, {'term': 'SU', 'year':2025, 'identifier':'SPSU'},{'term': 'SP', 'year':2025, 'identifier':'SU'},{'term': 'FW', 'year':2025, 'identifier':'FW'}])
    year = current_date.year
    if (current_date>date(year,8,24) and current_date<=date(year,12,31)):
        return ([{'term': 'FW', 'year':year, 'identifier':'FW'}])
    elif (current_date>=date(year,1,1) and current_date<date(year,3,27)):
        return ([{'term': 'FW', 'year':year-1, 'identifier':'FW'}])
    else:
        return ([{'term': 'SP', 'year':year, 'identifier':'SP'}, {'term': 'SU', 'year':year, 'identifier':'SPSU'}])


def get_data_hub_reports():
    # Loop through datasets in config
    for dataset in config['datasets']:
        
        schema_id = dataset["schema_id"]
        plugin_id = dataset["plugin_id"]

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
            filetype = d2l_functions.classify_location(row['Location'])
            download_syllabus(row, filetype)
            d2l_functions.upload_syllabus(row, filetype, access_token)
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
            d2l_functions.create_blank_syllabus(path)
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
now = time.time()
authorize_to_d2l = d2l_functions.trade_in_refresh_token(config)
access_token = authorize_to_d2l['access_token']
refresh_token = authorize_to_d2l['refresh_token']

if not access_token or not refresh_token:
    logger.error('Missing access or refresh token.')
    sys.exit(1)

d2l_functions.set_refresh_token(refresh_token, access_token, str(now))
logger.info('Tokens are set.')


# Download and extract data hub reports
datahub_path = 'datahub/'
os.makedirs(datahub_path, exist_ok=True)
logger.info('Downloading reports.')
get_data_hub_reports()
logger.info('Reports are downloaded.')


# Get database configuration
logger.info('Pushing reports into Database')
csv_db.setDb()
logger.info('Database updated.')

today = date.today()
term_year = get_academic_term(today)

logger.info('Current term identified.')
for each in term_year:
    year = each['year']
    term = each['term']

    #create folders in the Brightspace
    logger.info(f'Request for all course data initiated for given term: {term} and year: {year}.')
    all_courses = csv_db.get_sylabus(all_courses_query, term, year)

    logger.info('Creating folders in the BS')
    create_BS_folders(all_courses, year, term)

    logger.info('Generating folders in the server and html per Department->Year->Term.')
    d2l_functions.generate_syllabus_html(all_courses,base)
    
    logger.info('Uploading html files into Course Management area before creating modules.')
    d2l_functions.upload_content_html(all_courses, year, term, access_token)

    logger.info('Checking if Content Modules and Topics exists for given Departments->Years-Terms')
    add_content_module(all_courses, year, term)

    # Upload todays Sylabusses
    logger.info('Requesting syllabus data that are not been pushed to BS for given year and term.')
    syllabus_to_run = csv_db.get_sylabus(syllabus_query, term, year)
    logger.info('Downloading syllabuses and uploading them into Project sites.')
    download_upload_syllabus(syllabus_to_run)

    logger.info('Updating Recorded field in DB.')
    csv_db.update_syllabus_recorded(syllabus_to_run)

    logger.info('Setting Recorded=4 if Campus store status Complete')
    csv_db.campus_store_complete(year, term)

    logger.info('Setting Recorded=5 if the section type is in IGNORED_SECTION_TYPES')
    csv_db.mark_ignored_sections(year, term)

    logger.info('Requesting new all courses data for given term and year.')
    all_courses = csv_db.get_sylabus(all_courses_query,  term, year)
    logger.info('Generating folders and html files in the server again to update the html files with new records.')
    d2l_functions.generate_syllabus_html(all_courses, base)

    logger.info('Uploading updated html files to BS')
    d2l_functions.upload_content_html(all_courses, year, term, access_token)

logger.info('End.')

