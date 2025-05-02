import requests
from requests.auth import HTTPBasicAuth
import os
import zipfile
import urllib.parse
import mimetypes
from logger_config import logger
import dotenv
import pandas as pd
import api_auth
import re


dotenv_file = dotenv.find_dotenv()
dotenv.load_dotenv(dotenv_file)
bspace_url = os.environ["bspace_url"]

# Gets access (7200 seconds) and refresh tokens. Calls put_config() to update the refresh token in file.
def trade_in_refresh_token(config):
    try:
        response = requests.post(
            'https://auth.brightspace.com/core/connect/token',
            data={
                'grant_type': 'refresh_token',
                'refresh_token': config['refresh_token'],
                'scope': config['scope']
            },
            auth=HTTPBasicAuth(config['client_id'], config['client_secret'])
        )
        response.raise_for_status()
        response_data = response.json()
        return response_data
    except requests.exceptions.RequestException as e:
        logger.error(f"Error during token refresh: {e}")
        return None
    except KeyError:
        logger.error("Error: Unexpected response format.")
        return None


def set_refresh_token(refresh_token, access_token, timestamp):
    os.environ["refresh_token"] = refresh_token
    dotenv.set_key(dotenv_file, "refresh_token", os.environ["refresh_token"])

    os.environ["access_token"] = access_token
    dotenv.set_key(dotenv_file, "access_token", os.environ["access_token"])

    os.environ["timestamp"] = timestamp
    dotenv.set_key(dotenv_file, "timestamp", os.environ["timestamp"])

    dotenv.load_dotenv(dotenv_file)

# d2l GET call
def get_with_auth(endpoint, access_token):
    try:
        headers = {'Authorization': f'Bearer {access_token}'}
        response = requests.get(endpoint, headers=headers)
        response.raise_for_status()
        return response
    except requests.exceptions.RequestException as e:
        logger.error(f"Error during GET request: {e}")
        return None


def post_with_auth(endpoint, access_token, data=None, json_data=False):
    try:
        headers = {'Authorization': f'Bearer {access_token}'}

        if json_data:
            headers['Content-Type'] = 'application/json'
            response = requests.post(endpoint, headers=headers, json=data)  # Use `json=`
        else:
            headers['Content-Type'] = 'application/x-www-form-urlencoded'
            response = requests.post(endpoint, headers=headers, data=data)  # Use `data=`
        
        response.raise_for_status()
        return response
    except requests.exceptions.RequestException as e:
        logger.error(f"Error during POST request: {e}")
        return None


def save_file(url, access_token, download_path, code=None):
    try:
        # Fetch the file stream
        response = get_with_auth(url, access_token)
        
        if response and response.status_code == 200:
            # Extract filename from Content-Disposition header
            content_disposition = response.headers.get('Content-Disposition')
            if content_disposition and 'filename=' in content_disposition:
                # Extract the filename after 'filename='
                filename = content_disposition.split('filename=')[-1].strip(' "')
                # Decode URL-encoded filename
                filename = urllib.parse.unquote(filename) 
                
                # If `code` is provided, replace filename while keeping extension
                if code is not None:
                    _, file_extension = os.path.splitext(filename)  # Get the file extension
                    filename = f"syllabus_{code}{file_extension}"  # Rename file with new code 
                
                # Update the download path to use the extracted filename
                download_path = os.path.join(download_path, filename)
            else:
                logger.error("Error: Content-Disposition header does not contain a filename.")
                return None

            # Ensure directory exists
            os.makedirs(os.path.dirname(download_path), exist_ok=True)

            # Save the file content
            with open(download_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            logger.info(f"File saved successfully at {download_path}")
            return filename  # Return the extracted filename

        else:
            logger.error(f"Error: Received status code {response.status_code if response else 'None'}")
            if response and response.status_code == 403 and 'Retry-After' in response.headers:
                retry_after = response.headers['Retry-After']
                logger.error(f"Quota exceeded. Retry after {retry_after} seconds.")
            else:
                logger.error("Failed to download the file.")
            return None

    except IOError as e:
        logger.error(f"Error saving file: {e}")
        return None

def initiate_resumable_upload(base, upload_url, access_token, file_path, chunk_size = 1024 * 1024):
    # Ensure file exists
    if not os.path.exists(file_path):
        logger.error(f"Error: File '{file_path}' not found.")
        return None

    # Get file size
    file_size = os.path.getsize(file_path)

    # Auto-detect MIME type
    #mime_type = mimetypes.guess_type(file_path)[0] or "application/octet-stream"
    # Using generic mime type
    mime_type = "application/octet-stream"
    # Extract file name
    file_name = os.path.basename(file_path)

    # Construct headers
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": f"multipart/form-data;",
        "X-Upload-File-Name": file_name,
        "X-Upload-Content-Type": mime_type,
        "X-Upload-Content-Length": str(file_size),  # FIXED: Use actual length
    }

    # Send the PUT request with file data included
    response = requests.post(upload_url, headers=headers, allow_redirects=False)
    file_key = None
    if response.status_code == 308:
        with open(file_path, "rb") as file:
            start_byte = 0
            while response.status_code == 308:
                file.seek(start_byte)
                chunk = file.read(chunk_size)
                end_byte = start_byte + len(chunk) - 1

                headers = {
                    "Authorization": f"Bearer {access_token}",
                    "Content-Type": mime_type,
                    "Content-Range": f"bytes {start_byte}-{end_byte}/{file_size}"
                }
                upload_url = response.headers["Location"]
                if not upload_url:
                    logger.error("Upload URL not found in headers.")
                    return None
                file_key = os.path.basename(upload_url)
                response = requests.post(f"{base}{upload_url}", headers=headers, data=chunk, allow_redirects=False)
                if response.status_code == 308:  # Resume Incomplete
                    start_byte = int(response.headers.get("Range", f"bytes={end_byte}").split("-")[1]) + 1

    # Check response
    if response.status_code in [200, 201, 204]:  # Success status codes
        logger.info(f"Upload successful for {file_name}.")
        return file_key
    else:
        logger.error(f"Error: Unexpected response {response.status_code} - {response.text}")
    return None


def unzip_file(download_path, extract_to):
    try:
        # Extract the zip file
        with zipfile.ZipFile(download_path, 'r') as zip_ref:
            zip_ref.extractall(extract_to)
            logger.info(f"File extracted to {extract_to}")

        # Delete the zip file after extraction
        os.remove(download_path)
        logger.info(f"Deleted zip file {download_path}")

    except IOError as e:
        logger.error(f"Error extracting file: {e}")
    except zipfile.BadZipFile:
        logger.error("Error: The file is not a valid zip archive.")


def save_and_unzip_file(url, access_token, download_path):
    # Save the datahub file and get its name
    filename = save_file(url, access_token, download_path)

    if filename:
        full_path = os.path.join(os.path.dirname(download_path), filename)
        unzip_file(full_path, download_path)

def is_folder_exists(url, access_token, folder):
    response = get_with_auth(url, access_token)
    if response is not None:
        data = response.json()
        for obj in data.get("Objects", []):
            if obj.get("Name") == folder and obj.get("FileSystemObjectType") == 1:
                return True
    else:
        return False

def upload_syllabus(row, filetype, access_token):
    try:
        # Construct the URL with the row's Location value
        orgUnitId = row['ProjectId']
        location = str(row['Location']) if pd.notna(row['Location']) else ""
        department = str(row['Department'])
        year = str(row['Year'])
        term = str(row['Term'])

        upload_url = f"{bspace_url}/d2l/api/lp/1.47/{orgUnitId}/managefiles/file/upload"
        #file_name = os.path.basename(location)
        if filetype=='Link':
            pass
        else:
            _, file_extension = os.path.splitext(os.path.basename(location))
            if (filetype=='d2l'): file_extension = '.html'
            file_name = f"syllabus_{row['Code']}{file_extension}"
            file_path = f"downloads/{department}/{year}/{term}/{file_name}"
            file_key = initiate_resumable_upload(bspace_url, upload_url, access_token, file_path)
            if (file_key):
                save_file_payload = {"fileKey":file_key,
                                    "relativePath": f"{department}/{year}/{term}"}
                post_with_auth(f"{bspace_url}/d2l/api/lp/1.47/{orgUnitId}/managefiles/file/save?overwriteFile=true", access_token, data=save_file_payload, json_data=False)


    except Exception as e:
        logger.error(f"An error occurred: {e}")


def upload_content_html(df, year, term, access_token):
    grouped = df.groupby("Department").agg({
        'ProjectId': 'first'
    }).reset_index()

    for index, row in grouped.iterrows():
        orgUnitId = row['ProjectId']
        department = row['Department']
        upload_url = f"{bspace_url}/d2l/api/lp/1.47/{orgUnitId}/managefiles/file/upload"
        file_name = f"syllabus_table_{str(department)}_{str(year)}_{str(term)}.html"
        file_path = f"downloads/{department}/{year}/{term}/{file_name}"
        file_key = initiate_resumable_upload(bspace_url, upload_url, access_token, file_path)
        if (file_key):
            save_file_payload = {"fileKey":file_key,
                                 "relativePath": f"{department}/{year}/{term}"}
            post_with_auth(f"{bspace_url}/d2l/api/lp/1.47/{orgUnitId}/managefiles/file/save?overwriteFile=true", access_token, data=save_file_payload, json_data=False)



def generate_syllabus_html(df, base_output_dir):
    # Group by Department, Year, and Term
    grouped = df.groupby(["Department", "Year", "Term"])

    # Generate HTML files in each corresponding folder
    for (department, year, term), group in grouped:
        group = group.sort_values(by=["Duration","CourseNumber","Section"], ascending=True)
        # Count total courses and syllabuses recorded
        total_courses = len(group)
        recorded_syllabuses = (group['Recorded'].fillna(0).astype(int) != 0).sum()
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
            <link rel="stylesheet" href="{bspace_url}/shared/Widgets/SyllabusUpload/css/syllabus_collection_styles.css" />
            <script src="https://cdn.datatables.net/2.2.2/js/dataTables.js"></script>    
            <title>Syllabus Table for {department} - {year} - {term}</title>
        </head>
        <body>
            <h2>Syllabus for {department} - {year} - {term}</h2>
            <p>Total Courses: {total_courses}, Syllabuses Available or Exempted: {recorded_syllabuses} ({recorded_percentage:.2f}%)</p>
            <p><a href="https://cpi.brocku.ca/api/report?department={department}&year={year}&term={term}&token={api_auth.generate_token(f'{department}-{year}-{term}')}" class="download-report">Download Report</a></p>
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
            if pd.isna(row['Code']):
                logger.warning("Missing course Code; skipping row.")
                continue
            exempt_value = 'exempt'
            # Handle NaN values in Recorded
            row['Recorded'] = 0 if pd.isna(row['Recorded']) else int(row['Recorded'])

            if row['Recorded']==1:
                if pd.isna(row['Location']):
                    syllabus_link = row['Code']
                elif classify_location(row['Location']) == 'Link':
                    syllabus_link = f"<a href={row['Location']} target='_blank'>{row['Code']}</a>"
                else:
                    _, file_extension = os.path.splitext(os.path.basename(str(row['Location'])))
                    if classify_location(row['Location']) == 'd2l':
                        file_extension = '.html'
                    href = f"/content/enforced/{row['ProjectId']}-Project-{row['ProjectId']}-PSPT/{row['Department']}/{row['Year']}/{row['Term']}/syllabus_{row['Code']}{file_extension}"
                    syllabus_link = f"<a href={href} target='_blank'>{row['Code']}</a>"
            elif row['Recorded']==2:
                syllabus_link = f"{row['Code']} (exempted)"
                exempt_value = 'unexempt'
            else: 
                syllabus_link = row['Code']

            url_token = api_auth.generate_token(row['Code'])
            upload_url = f"https://cpi.brocku.ca/api/upload?course={row['Code']}&token={url_token}&projectId={row['ProjectId']}"
            exempt_url = f"https://cpi.brocku.ca/api/exempt?course={row['Code']}&token={url_token}&action={exempt_value}"

            html_content += f"""
                <tr>
                    <td>{syllabus_link}</td>
                    <td>
                        <button class="icon-btn upload" title="Upload" data-url="{upload_url}"></button>
                        <button class="icon-btn exempt {exempt_value}" title="Exempt" data-url="{exempt_url}"></button>
                    </td>
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
            }});</script>
            <script src="{bspace_url}/shared/Widgets/SyllabusUpload/js/syllabus_collection.js"></script>
        </body>
        </html>
        """
        # Write to file
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(html_content)


def create_blank_syllabus(path):
    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Unavailable Syllabus</title>
        <link rel="stylesheet" href="{bspace_url}/shared/Widgets/SyllabusUpload/css/syllabus_collection_styles.css" />
    </head>
    <body>
            <h3>Unavailable Syllabus.</h3>
            <p>The syllabus for this course cannot be retrieved from its current Brightspace location (e.g., Discussions, Assignments, etc). Please contact the instructor to have them upload the syllabus to the Content area of Brightspace instead.</p>
    </body>
    </html>
    """
    with open(path, 'w', encoding='utf-8') as f:
        f.write(html_content)


def classify_location(value):
    value = str(value).strip()
    if re.match(r'^https?://', value, re.IGNORECASE):
        return 'Link'

    if value.lower().startswith('d2l') or value.startswith('/d2l/'):
        return 'd2l'

    return None