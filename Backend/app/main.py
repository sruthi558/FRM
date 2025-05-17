# /Backend/app/main.py
from flask import Flask, request, jsonify,request
import requests

from flask_sqlalchemy import SQLAlchemy
from flask_bcrypt import Bcrypt
from flask_jwt_extended import (JWTManager, create_access_token, decode_token,jwt_required, get_jwt_identity)
from flask_cors import CORS
from flask_mail import Mail, Message
from threading import Thread
import psycopg2
from psycopg2.extras import DictCursor
import pandas as pd
import os
import schedule
import time
import threading
from apscheduler.schedulers.background import BackgroundScheduler
import base64
from datetime import date,datetime,timedelta
app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://postgres:12345@localhost:5432/FRM'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['JWT_SECRET_KEY'] = 'your_jwt_secret_key'
app.config['MAIL_SERVER'] = 'smtp.gmail.com'
app.config['MAIL_PORT'] = 587
app.config['MAIL_USE_TLS'] = True
app.config['MAIL_USERNAME'] = 'durga@corundumtechnologies.com'
app.config['MAIL_PASSWORD'] = 'yaviezhynflcygvs'
app.config['AUTH_SERVICE_URL'] = os.getenv('AUTH_SERVICE_URL', 'http://localhost:5001')
app.config['FLOW_URL'] = os.getenv('FLOW_URL', 'http://localhost:5002')
app.config['QUERIES_URL'] = os.getenv('QUERIES_URL', 'http://localhost:5004')
app.config['MAIL_USE_SSL'] = False
UPLOAD_FOLDER = 'uploads'
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER 
db = SQLAlchemy(app)
bcrypt = Bcrypt(app)
jwt = JWTManager(app)
mail = Mail(app) 
CORS(app)
# Initialize db with app
class UserDetails(db.Model):
    __tablename__ = 'user_details'
    id = db.Column(db.Integer, primary_key=True)
    emp_id = db.Column(db.String(255), nullable=False)
    username = db.Column(db.String(255), unique=True, nullable=False)
    email_id = db.Column(db.String(255), unique=True, nullable=False)
    mobile_no = db.Column(db.String(15), nullable=False)
    address = db.Column(db.String(255), nullable=False)
    role = db.Column(db.String(50), nullable=False)
    status = db.Column(db.String(25), nullable=False)
    telephone_no = db.Column(db.String(20), nullable=False)
    branch = db.Column(db.String(100), nullable=False)
    branch_code = db.Column(db.String(15),nullable=False)
    branch_region = db.Column(db.String(15), nullable=False)
    password = db.Column(db.String(255), nullable=True)
    is_admin = db.Column(db.Boolean, default=False)
    code=db.Column(db.Integer,nullable=True)
    def __repr__(self):
        return f'<UserDetails {self.username}>'

# Routes
@app.route('/', methods=['POST'])
def login_user():
    print("1")
    credentials = request.get_json()
    print(credentials, "credentials")
    try:
        response = requests.post(f"{app.config['AUTH_SERVICE_URL']}/", json=credentials)
        print(response)
        return jsonify(response.json()), response.status_code
    except Exception as e:
        return jsonify({'error': str(e)}), 500
@app.route('/add_user', methods=['POST'])

@jwt_required()
def add_user():
    print("2")

    token = request.headers.get('Authorization')
    if token is None:
        return jsonify({'error': 'Authorization token missing'}), 401
    user_data = request.get_json()
    try:
        headers = {'Authorization': token}
        print(headers)
        response = requests.post(f"{app.config['AUTH_SERVICE_URL']}/add_user", json=user_data, headers=headers)
        return jsonify(response.json()), response.status_code
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/get_user', methods=['GET'])
@jwt_required()
def get_logged_in_user():
    print("3")
    token = request.headers.get('Authorization')
    if not token:
        return jsonify({'error': 'Authorization token missing'}), 401
    try:
        headers = {'Authorization': token}
        response = requests.get(f"{app.config['AUTH_SERVICE_URL']}/get_user", headers=headers)
        
        if response.status_code == 200:
            return jsonify(response.json()), response.status_code
        else:
            return jsonify({"message": "Error fetching user data"}), response.status_code
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    
@app.route('/view_users', methods=['GET'])
@jwt_required()
def view_users():
    print("4")
    token = request.headers.get('Authorization')
    print("here123")
    if not token:
        return jsonify({'error': 'Authorization token missing'}), 401
    try:
        headers = {'Authorization': token}
        response = requests.get(f"{app.config['AUTH_SERVICE_URL']}/view_users", headers=headers)
        
        if response.status_code == 200:
            return jsonify(response.json()), response.status_code
        else:
            return jsonify({"message": "Error fetching user data"}), response.status_code
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/get_user/<int:user_id>', methods=['GET'])
@jwt_required()
def get_user(user_id):
    print("here")
    print("5")
    token = request.headers.get('Authorization')
    if not token:
        return jsonify({'error': 'Authorization token missing'}), 401
    try:
        headers = {'Authorization': token}
        response = requests.get(f"{app.config['AUTH_SERVICE_URL']}/get_user/<int:user_id>", headers=headers)
        
        if response.status_code == 200:
            return jsonify(response.json()), response.status_code
        else:
            return jsonify({"message": "Error fetching user data"}), response.status_code
    except Exception as e:
        return jsonify({'error': str(e)}), 500
@app.route('/update_user/<int:user_id>', methods=['GET'])
@jwt_required()
def update_user(user_id):
    print("6")
    print("here122")
    token = request.headers.get('Authorization')
    if not token:
        return jsonify({'error': 'Authorization token missing'}), 401
    try:
        headers = {'Authorization': token}
        response = requests.get(f"{app.config['AUTH_SERVICE_URL']}/update_user(user_id)", headers=headers)
        
        if response.status_code == 200:
            return jsonify(response.json()), response.status_code
        else:
            return jsonify({"message": "Error fetching user data"}), response.status_code
    except Exception as e:
        return jsonify({'error': str(e)}), 500
@app.route('/verify', methods=['POST'])
def verify_code():
    try:
        print("here")
        print("7")
        data = request.get_json()
        print(data)
        response = requests.post(f"{app.config['AUTH_SERVICE_URL']}/verify", json=data)
        print(response)
        return jsonify(response.json()), response.status_code
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# @app.route('/upload_file', methods=['POST'])
# def upload_file():
#     print("8")

#     if 'file' not in request.files:
#         return jsonify({"message": "No file part"}), 400
#     file = request.files['file']
#     try:
#         response = requests.post(
#             f"{app.config['FLOW_URL']}/upload_file",
#             files={'file': (file.filename, file.stream, file.mimetype)}
#         )
#         return jsonify(response.json()), response.status_code
#     except Exception as e:
#         return jsonify({'error': str(e)}), 500


# @app.route('/check_upload_button', methods=['GET'])
# def check_upload_button():
#     print("9")
#     try:
#         response = requests.get(f"{app.config['FLOW_URL']}/check_upload_button")
#         return jsonify(response.json()), response.status_code
    
#     except Exception as e:
#         return jsonify({"error": str(e)}), 500
 
@app.route('/fetch_matched_data_from_output_table')
@jwt_required()
def fetch_matched_data_from_output_table():
    print("9")
    print("here1232")
    token = request.headers.get('Authorization')
    if not token:
        return jsonify({'error': 'Authorization token missing'}), 401
    try:
        headers = {'Authorization': token}
        response = requests.get(f"{app.config['FLOW_URL']}/fetch_matched_data_from_output_table", headers=headers)
        print(response,"res")
        return jsonify(response.json()), response.status_code
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/update_comment/<int:unique_id>', methods=['POST'])
@jwt_required()
def update_comment(unique_id):
    print("11")
    token = request.headers.get('Authorization')
    if not token:
        return jsonify({'error': 'Authorization token missing'}), 401
    try:
        headers = {'Authorization': token}
        response = requests.get(f"{app.config['FLOW_URL']}/update_comment/<int:unique_id>",headers=headers)
        return jsonify(response.json()), response.status_code
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/customer', methods=['POST'])
def customer():
    data = request.get_json() 
    customer_id = data.get('customer_id')  
    if not customer_id:
        return jsonify({'error': 'Customer ID not provided'}), 400
    headers = {
        'Content-Type': 'application/json',
    }
    try:
        response = requests.post(f'http://localhost:5003/api/customer', json=data, headers=headers)    
        return jsonify(response.json()), response.status_code
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/submit_form/<int:unique_id>', methods=['POST'])
@jwt_required()
def submit_form(unique_id):
    token = request.headers.get('Authorization')
    if not token:
        return jsonify({'error': 'Authorization token missing'}), 401
    try:
        headers = {'Authorization': token}
        response = requests.get(f"{app.config['FLOW_URL']}/submit_form/<int:unique_id>",headers=headers)
        return jsonify(response.json()), response.status_code
    except Exception as e:
        return jsonify({"error": str(e)}), 500
@app.route('/upload_file', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return jsonify({"message": "No file part"}), 400
    file = request.files['file']
    if file.filename == '':
        return jsonify({"message": "No selected file"}), 400
    try:
        response = requests.post(
            f"{app.config['QUERIES_URL']}/upload_file",
            files={'file': (file.filename, file.stream, file.mimetype)}
        )
        print(response,"response")
        return jsonify(response.json()), response.status_code
    except Exception as e:
        return jsonify({"message": f"Failed to upload file: {str(e)}"}), 500
    

if __name__ == '__main__':
    app.run(host="0.0.0.0",debug=True, port=5000)
