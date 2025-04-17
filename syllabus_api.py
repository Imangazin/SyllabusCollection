from flask import Flask, request, jsonify, abort
from flask_cors import CORS
import api_auth
import dotenv
import os
from logger_config import logger
import csv_db
import d2l_functions
import pandas as pd
import time

dotenv_file = dotenv.find_dotenv()
dotenv.load_dotenv(dotenv_file)
origin = os.environ["bspace_url"]

def get_config():
    return {
        "bspace_url": os.environ["bspace_url"],
        "client_id": os.environ["client_id"],
        "client_secret": os.environ["client_secret"],
        "scope": os.environ["scope"],
        "refresh_token": os.environ["refresh_token"],
        "access_token": os.environ["access_token"],
        "timestamp": os.environ["timestamp"]
    }

def get_access_token():
    now = time.time()
    config = get_config()
    buffer_seconds = 300
    expires_in = 7200
    if (config["access_token"] is None or (now - float(config["timestamp"])) >= (expires_in - buffer_seconds)):
        authorize_to_d2l = d2l_functions.trade_in_refresh_token(config)
        access_token = authorize_to_d2l['access_token']
        refresh_token = authorize_to_d2l['refresh_token']
        d2l_functions.set_refresh_token(refresh_token, access_token, str(now))
        return access_token
    return config["access_token"]



app = Flask(__name__)
CORS(app, origins=[origin])


@app.route('/api/upload', methods=['POST'])
def upload():
    course_code = request.args.get('course')
    token = request.args.get('token')

    
    if not course_code or not token or not api_auth.verify_token(course_code, token):
        logger.error('api/upload: Invalid or missing signature')
        abort(403, 'Invalid or missing signature')

    # uploaded_file = request.files.get('file')
    # if not uploaded_file:
    #     abort(400, 'No file uploaded')

    # # Save the file
    # uploaded_file.save(f"./uploads/syllabus_{course}.txt")
    return jsonify({"status": "success", "message": f"File uploaded for {course_code}"}), 200

@app.route('/api/exempt', methods=['POST'])
def exempt():
    course_code = request.args.get('course')
    token = request.args.get('token')

    if not course_code or not token or not api_auth.verify_token(course_code, token):
        logger.error('api/exempt: Invalid or missing signature')
        abort(403, 'Invalid or missing signature')

    # Will be creating blank html page
    year, term, department = extract_info(course_code)
    orgUnitId = csv_db.get_orgUnitId_by_code(course_code)
    print(f'OrgUnitId: {orgUnitId}')
    # # Update the DB, mark the course as exempted by changing Recorded field value to 2.
    # exempt_df = pd.DataFrame([{'OrgUnitId': int(orgUnitId)}])
    # csv_db.update_syllabus_recorded(exempt_df, 2)

    # # Recreate the html file
    # department_courses_df = csv_db.get_department_cources(term, year, department)
    # d2l_functions.generate_syllabus_html(department_courses_df, 'downloads')

    # #Upload it to BS
    # access_token = get_access_token()
    # d2l_functions.upload_content_html(department_courses_df, year, term, access_token)

    return jsonify({"status": "success", "message": f"{course_code} has been exempted. OrgUnitID={orgUnitId}"}), 200


def extract_info(string):
    parts = string.split('-')
    year = parts[0]
    term = parts[1]
    code = parts[4]
    return year, term, code



if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)