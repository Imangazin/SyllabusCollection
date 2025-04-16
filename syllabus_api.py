from flask import Flask, request, jsonify, abort
from flask_cors import CORS
import api_auth

app = Flask(__name__)
CORS(app, origins=["https://brocktest.brightspace.com"])

@app.route('/api/upload', methods=['POST'])
def upload():
    course = request.args.get('course')
    token = request.args.get('token')
    
    if not course or not token or not api_auth.verify_token(course, token):
        abort(403, 'Invalid or missing signature')

    # uploaded_file = request.files.get('file')
    # if not uploaded_file:
    #     abort(400, 'No file uploaded')

    # # Save the file
    # uploaded_file.save(f"./uploads/syllabus_{course}.txt")
    return jsonify({"status": "success", "message": f"File uploaded for {course}"}), 200

@app.route('/api/exempt', methods=['POST'])
def exempt():
    print("Exempt API was called")
    course = request.args.get('course')
    token = request.args.get('token')

    if not course or not token or not api_auth.verify_token(course, token):
        abort(403, 'Invalid or missing signature')


    # Perform exemption logic here
    return jsonify({"status": "success", "message": f"{course} has been exempted"}), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)