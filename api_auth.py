import hmac
import hashlib
import base64

import dotenv
import os

dotenv_file = dotenv.find_dotenv()
dotenv.load_dotenv(dotenv_file)
SECRET_KEY = os.environ["secret_key"]

def generate_token(course_code):
    message = course_code.encode('utf-8')
    key = SECRET_KEY.encode('utf-8')
    sig = hmac.new(key, message, hashlib.sha256).digest()
    return base64.urlsafe_b64encode(sig).decode('utf-8')

def verify_token(course_code, token):
    expected_token = generate_token(course_code)
    return hmac.compare_digest(expected_token, token)