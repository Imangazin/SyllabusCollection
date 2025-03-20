import requests
from requests.auth import HTTPBasicAuth
import os
import zipfile
import urllib.parse
import mimetypes
from logger_config import logger

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
    mime_type = mimetypes.guess_type(file_path)[0] or "application/octet-stream"

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


