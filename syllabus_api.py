from flask import Flask, request, jsonify, abort, send_file
from flask_cors import CORS
import api_auth
import dotenv
import os
from logger_config import logger
import csv_db
import d2l_functions
import pandas as pd
import time
import requests
import io


dotenv_file = dotenv.find_dotenv()
dotenv.load_dotenv(dotenv_file)
origin = os.environ["bspace_url"]


def get_config():
    dotenv.load_dotenv(dotenv_file, override=True)
    return {
        "bspace_url": os.environ["bspace_url"],
        "client_id": os.environ["client_id"],
        "client_secret": os.environ["client_secret"],
        "scope": os.environ["scope"],
        "refresh_token": os.environ["refresh_token"],
        "access_token": os.environ["access_token"],
        "timestamp": os.environ["timestamp"],
    }


def get_access_token():
    now = time.time()
    config = get_config()
    buffer_seconds = 300
    expires_in = 7200

    if (
        config["access_token"] is None
        or (now - float(config["timestamp"])) >= (expires_in - buffer_seconds)
    ):
        authorize_to_d2l = d2l_functions.trade_in_refresh_token(config)
        if not authorize_to_d2l:
            logger.error("Failed to refresh token.")
            abort(500, "Internal server error: token refresh failed.")

        access_token = authorize_to_d2l["access_token"]
        refresh_token = authorize_to_d2l["refresh_token"]
        d2l_functions.set_refresh_token(refresh_token, access_token, str(now))

        dotenv.load_dotenv(dotenv_file, override=True)
        return access_token

    return config["access_token"]


def pct(n, d):
    if not d:
        return 0.0
    return (float(n) / float(d)) * 100.0


def make_stats_row(year, terms, faculty_id, label):
    c = csv_db.fetch_counts(year, terms, faculty_id)
    return {
        "label": label,
        "raw_collected": c["recorded"],
        "raw_total": c["total"],
        "raw_pct": pct(c["recorded"], c["total"]),
        "qualified_collected": c["qualified_recorded"],
        "qualified_total": c["qualified_total"],
        "qualified_pct": pct(c["qualified_recorded"], c["qualified_total"]),
    }


app = Flask(__name__)
# CORS(app, resources={r"/api/*": {"origins": origin}})
logger.info(f"Origin: {origin}")
app.config["MAX_CONTENT_LENGTH"] = 2 * 1024 * 1024 * 1024  # 2 GB


@app.route("/api/stats", methods=["GET"])
def api_stats():
    faculty_id = request.args.get("facultyId")
    token = request.args.get("token")
    if not faculty_id or not token or not api_auth.verify_token(faculty_id, token):
        logger.error("api/stats: Invalid or missing signature")
        abort(403, "Invalid or missing signature")

    years = [y[0] for y in csv_db.get_last_three_years()]
    sections = {"full_year": [], "fw": [], "sp": [], "su": []}

    for y in years:
        sections["full_year"].append(make_stats_row(y, ("FW", "SP", "SU"), faculty_id, label=str(y)))
        sections["fw"].append(make_stats_row(y, ("FW",), faculty_id, label="{}-FW".format(y)))
        sections["sp"].append(make_stats_row(y, ("SP",), faculty_id, label="{}-SP".format(y)))
        sections["su"].append(make_stats_row(y, ("SU",), faculty_id, label="{}-SU".format(y)))

    return jsonify(sections)


@app.route("/api/stats/by-department", methods=["GET"])
def api_stats_by_department():
    faculty_id = request.args.get("facultyId")
    token = request.args.get("token")
    if not faculty_id or not token or not api_auth.verify_token(faculty_id, token):
        logger.error("api/stats/by-department: Invalid or missing signature")
        abort(403, "Invalid or missing signature")

    years = [y[0] for y in csv_db.get_last_three_years()]
    data = csv_db.fetch_department_count(years, faculty_id)

    # pivot to: Department | year1 | year2 | year3
    by_dept = {}
    for dept, year, q_total, q_recorded in data:
        dept = str(dept)
        year = str(year)
        q_total = int(q_total or 0)
        q_recorded = int(q_recorded or 0)
        pct_val = 0.0 if q_total == 0 else (float(q_recorded) / float(q_total) * 100.0)

        if dept not in by_dept:
            by_dept[dept] = {"department": dept}
        by_dept[dept][year] = pct_val

    year_keys = [str(y) for y in years]
    rows = []
    for dept in sorted(by_dept.keys()):
        r = by_dept[dept]
        for y in year_keys:
            if y not in r:
                r[y] = 0.0
        rows.append(r)

    return jsonify({"years": years, "rows": rows})


@app.route("/api/upload", methods=["POST"])
def upload():
    try:
        course_code = request.args.get("course")
        token = request.args.get("token")
        projectId = request.args.get("projectId")

        if not course_code or not token or not api_auth.verify_token(course_code, token):
            logger.error("api/upload: Invalid or missing signature")
            abort(403, "Invalid or missing signature")

        uploaded_file = request.files.get("file")
        if not uploaded_file:
            abort(400, "No file uploaded")

        year, term, department = extract_info(course_code)
        logger.debug(f"year: {year}, term: {term}, department: {department}")
        orgUnitId = csv_db.get_orgUnitId_by_code(course_code)
        if orgUnitId is None:
            logger.error(f"orgUnitId not found for course_code: {course_code}")
            abort(400, f"Course code not found in database: {course_code}")

        original_filename = uploaded_file.filename
        if not original_filename:
            abort(400, "Uploaded file has no filename")
        _, file_extension = os.path.splitext(original_filename)

        new_filename = f"syllabus_{course_code}{file_extension}"

        upload_folder = f"downloads/{department}/{year}/{term}"
        os.makedirs(upload_folder, exist_ok=True)
        file_path = os.path.join(upload_folder, new_filename)
        uploaded_file.save(file_path)

        access_token = get_access_token()
        row = {
            "ProjectId": projectId,
            "Code": course_code,
            "Location": original_filename,
            "Department": department,
            "Year": year,
            "Term": term,
        }
        logger.info(f"Received file: {uploaded_file.filename}, type: {uploaded_file.mimetype}")
        d2l_functions.upload_syllabus(row, None, access_token)

        upload_df = pd.DataFrame([{ "OrgUnitId": orgUnitId }])
        csv_db.update_syllabus_recorded(upload_df)
        csv_db.upsert_content_object(None, orgUnitId, new_filename, "Topic", new_filename, None, 0)

        department_courses_df = csv_db.get_department_cources(term, year, department)
        d2l_functions.generate_syllabus_html(department_courses_df, "downloads")
        d2l_functions.upload_content_html(department_courses_df, year, term, access_token)

        logger.info(f"Syllabus uploaded for course {course_code} saved as {new_filename} at {file_path}")
        return jsonify({"status": "success", "message": f"{course_code} syllabus uploaded."}), 200

    except Exception as e:
        logger.error(f"Upload failed: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/api/exempt", methods=["POST"])
def exempt():
    course_code = request.args.get("course")
    token = request.args.get("token")
    exempt_value = request.args.get("action")

    if not course_code or not token or not api_auth.verify_token(course_code, token):
        logger.error("api/exempt: Invalid or missing signature")
        abort(403, "Invalid or missing signature")

    year, term, department = extract_info(course_code)
    orgUnitId = csv_db.get_orgUnitId_by_code(course_code)

    exempt_df = pd.DataFrame([{ "OrgUnitId": orgUnitId }])
    if exempt_value == "exempt":
        csv_db.update_syllabus_recorded(exempt_df, 2)
    elif exempt_value == "unexempt":
        csv_db.update_syllabus_recorded(exempt_df, 0)

    department_courses_df = csv_db.get_department_cources(term, year, department)
    d2l_functions.generate_syllabus_html(department_courses_df, "downloads")

    access_token = get_access_token()
    d2l_functions.upload_content_html(department_courses_df, year, term, access_token)
    logger.info(f"Syllabus exempted for course {course_code} successfully.")

    return (
        jsonify({"status": "success", "message": f"{course_code} has been exempted. OrgUnitID={orgUnitId}"}),
        200,
    )


@app.route("/api/report", methods=["GET"])
def getReport():
    department = request.args.get("department")
    year = request.args.get("year")
    term = request.args.get("term")
    token = request.args.get("token")

    if (
        not department
        or not year
        or not term
        or not token
        or not api_auth.verify_token(f"{department}-{year}-{term}", token)
    ):
        logger.error("api/exempt: Invalid or missing signature")
        abort(403, "Invalid or missing signature")

    department_courses_df = csv_db.get_department_cources(term, year, department)
    report_data = department_courses_df[["Code", "Recorded"]].copy()

    def map_recorded_status(value):
        if value == 1:
            return "Uploaded"
        if value == 2:
            return "Exempted"
        return ""

    report_data["Recorded"] = report_data["Recorded"].apply(map_recorded_status)
    logger.info(f"Report is sent to front-end for {department}-{year}-{term}")
    return jsonify(report_data.to_dict(orient="records")), 200


@app.route('/api/report/academic-year', methods=['GET'])
def api_report_academic_year():

    faculty_id = request.args.get('facultyId')
    token = request.args.get('token')
    year = request.args.get('year')

    if not faculty_id or not token or not api_auth.verify_token(faculty_id, token):
        logger.error('api/report/academic-year: Invalid or missing signature')
        abort(403, 'Invalid or missing signature')

    if not year:
        abort(400, 'Missing required parameter: year')

    terms = ('FW', 'SP', 'SU')

    full_counts = csv_db.fetch_counts(year, terms, faculty_id)
    full_year_df = pd.DataFrame([{
        'Academic Year': str(year),
        'Terms': 'FW+SP+SU',
        # 'Raw Collected': int(full_counts.get('recorded', 0) or 0),
        # 'Raw Total Courses': int(full_counts.get('total', 0) or 0),
        # 'Raw % Complete': pct(int(full_counts.get('recorded', 0) or 0), int(full_counts.get('total', 0) or 0)),
        # 'Qualified Collected': int(full_counts.get('qualified_recorded', 0) or 0),
        'Qualified Total Courses': int(full_counts.get('qualified_total', 0) or 0),
        'Qualified % Complete': pct(int(full_counts.get('qualified_recorded', 0) or 0), int(full_counts.get('qualified_total', 0) or 0)),
    }])

    by_term_rows = []
    for t in terms:
        c = csv_db.fetch_counts(year, (t,), faculty_id)
        by_term_rows.append({
            'Academic Year': str(year),
            'Term': t,
            # 'Raw Collected': int(c.get('recorded', 0) or 0),
            # 'Raw Total Courses': int(c.get('total', 0) or 0),
            # 'Raw % Complete': pct(int(c.get('recorded', 0) or 0), int(c.get('total', 0) or 0)),
            'Qualified Collected': int(c.get('qualified_recorded', 0) or 0),
            'Qualified Total Courses': int(c.get('qualified_total', 0) or 0),
            'Qualified % Complete': pct(int(c.get('qualified_recorded', 0) or 0), int(c.get('qualified_total', 0) or 0)),
        })
    by_term_df = pd.DataFrame(by_term_rows)

    dept_rows = []
    for dept, y, q_total, q_recorded in csv_db.fetch_department_count([year], faculty_id):
        q_total = int(q_total or 0)
        q_recorded = int(q_recorded or 0)
        dept_rows.append({
            'Department': str(dept),
            'Academic Year': str(y),
            'Qualified Collected': q_recorded,
            'Qualified Total Courses': q_total,
            'Qualified % Complete': pct(q_recorded, q_total),
        })
    by_dept_df = pd.DataFrame(dept_rows)

    courses_df = csv_db.fetch_academic_year_courses(year, terms, faculty_id)

    def map_recorded_status(v):
        try:
            v = int(v)
        except Exception:
            return ''
        if v == 1:
            return 'Uploaded'
        if v == 2:
            return 'User Exempted'
        if v == 4:
            return 'Campus Store Complete'
        if v == 5:
            return 'Auto Exempted'
        return ''

    if not courses_df.empty and 'Recorded' in courses_df.columns:
        courses_df['SyllabusStatus'] = courses_df['Recorded'].apply(map_recorded_status)

    # Rename AdoptionStatus column for report clarity
    if 'AdoptionStatus' in courses_df.columns:
        courses_df = courses_df.rename(columns={'AdoptionStatus': 'Book Store Status'})

    # Do not include 'Recorded' in the report output
    preferred = ['Year', 'Term', 'Department', 'Code', 'Book Store Status', 'SyllabusStatus']

    course_cols = [c for c in preferred if c in courses_df.columns] + \
                  [c for c in courses_df.columns if c not in preferred and c != 'Recorded']

    courses_df = courses_df[course_cols] if not courses_df.empty else pd.DataFrame(columns=preferred)

    filename = f"syllabus_report_{year}.xlsx"

    bio = io.BytesIO()
    with pd.ExcelWriter(bio, engine='openpyxl') as writer:
        sheet = 'report'

        # Section 1: Full Year
        startrow = 1
        full_year_df.to_excel(writer, index=False, sheet_name=sheet, startrow=startrow)
        ws = writer.sheets[sheet]
        ws.cell(row=startrow, column=1, value='Full Year (FW+SP+SU)')

        # Section 2: By Term
        startrow = startrow + len(full_year_df) + 4
        by_term_df.to_excel(writer, index=False, sheet_name=sheet, startrow=startrow)
        ws.cell(row=startrow, column=1, value='By Term')

        # Section 3: By Department
        startrow = startrow + len(by_term_df) + 4
        by_dept_df.to_excel(writer, index=False, sheet_name=sheet, startrow=startrow)
        ws.cell(row=startrow, column=1, value='By Department')

        # Section 4: Courses
        startrow = startrow + len(by_dept_df) + 4
        courses_df.to_excel(writer, index=False, sheet_name=sheet, startrow=startrow)
        ws.cell(row=startrow, column=1, value='Courses')

        # Freeze the first row
        ws.freeze_panes = 'A2'

        # Remove the default empty sheet created by openpyxl (commonly named 'Sheet')
        for name in list(writer.book.sheetnames):
            if name != sheet and name.lower().startswith('sheet'):
                del writer.book[name]

    bio.seek(0)
    return send_file(
        bio,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        as_attachment=True,
        download_name=filename,
    )


def extract_info(string):
    parts = string.split('-')
    if len(parts) < 5:
        raise ValueError(f"Invalid course code format: {string}")
    year = parts[0]
    term = parts[1]
    code = parts[4]
    return year, term, code


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)