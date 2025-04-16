import hmac
import hashlib
import base64


def generate_token(course_code, secret_key):
    message = course_code.encode('utf-8')
    key = secret_key.encode('utf-8')
    sig = hmac.new(key, message, hashlib.sha256).digest()
    return base64.urlsafe_b64encode(sig).decode('utf-8')

def verify_token(course_code, token):
    expected_token = generate_token(course_code)
    return hmac.compare_digest(expected_token, token)