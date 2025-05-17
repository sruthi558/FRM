from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy
from flask_bcrypt import Bcrypt
from flask_jwt_extended import (JWTManager, create_access_token, decode_token,jwt_required, get_jwt_identity)
from flask_cors import CORS
from flask_mail import Mail, Message
import email
import imaplib
from email.header import decode_header
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
import atexit
from datetime import date,datetime,timedelta
from flask import current_app as app
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR
import re
import mail
from io import BytesIO  

import os
from dotenv import load_dotenv

load_dotenv()
app = Flask(__name__)
CORS(app)

app.config['SQLALCHEMY_DATABASE_URI'] = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['JWT_SECRET_KEY'] = 'your_jwt_secret_key'
app.config['MAIL_SERVER'] = 'smtp.gmail.com'
app.config['MAIL_PORT'] = 587
app.config['MAIL_USE_TLS'] = True
app.config['MAIL_USERNAME'] = 'durga@corundumtechnologies.com'
app.config['MAIL_PASSWORD'] = 'yaviezhynflcygvs'
app.config['MAIL_DEFAULT_SENDER'] = 'your-email@gmail.com'
app.config['IMAP_SERVER'] = 'imap.gmail.com'
app.config['IMAP_PORT'] = 993
app.config['EMAIL_ACCOUNT'] = 'durga@corundumtechnologies.com'
app.config['EMAIL_PASSWORD'] = 'yaviezhynflcygvs'
UPLOAD_FOLDER = 'uploads'
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

db = SQLAlchemy(app)
jwt = JWTManager(app)
mail = Mail(app)
dbname = os.getenv('DB_NAME')
user = os.getenv('DB_USER')
password = os.getenv('DB_PASSWORD')
host = os.getenv('DB_HOST')
port = os.getenv('DB_PORT')
app.config['UPLOAD_FOLDER'] = 'uploads/'
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
    password = db.Column(db.String(255), nullable=True)     
    is_admin = db.Column(db.Boolean, default=False)
    branch_region = db.Column(db.String(100), nullable=False)
    def __repr__(self):
        return f'<UserDetails {self.username}>'
class OutputTable(db.Model):
    __tablename__ = 'output_table'
    unique_id = db.Column(db.Integer, primary_key=True)
    comments_by_ro_maker = db.Column(db.String(500))
    date_updated_by_ro_maker = db.Column(db.DateTime)
    comments_by_ro_checker = db.Column(db.String(500))
    date_updated_by_ro_checker = db.Column(db.DateTime)
    comments_by_co_maker=db.Column(db.String(500))
    date_updated_by_co_maker = db.Column(db.DateTime)
    co_maker_status = db.Column(db.String(1000))
    co_maker_action= db.Column(db.String(1000))
    co_maker_scn_status = db.Column(db.String(500))
    co_maker_scn_comments = db.Column(db.String(1000))
    co_maker_scn_date_updated = db.Column(db.DateTime)
    comments_by_co_checker=db.Column(db.String(500))
    date_updated_by_co_checker = db.Column(db.DateTime)
    co_checker_status=db.Column(db.String(1000))
    co_checker_action=db.Column(db.String(1000))
    co_checker_scn_action = db.Column(db.String(500))
    co_checker_scn_comments = db.Column(db.String(1000))
    co_checker_scn_date_updated = db.Column(db.DateTime)
    frm_cell_head=db.Column(db.String(500))
    reply_body = db.Column(db.Text)
    ro_maker_files=db.Column(db.JSON)
    ro_checker_files=db.Column(db.JSON)
    co_maker_files = db.Column(db.JSON)
    co_checker_files=db.Column(db.JSON)
    attachments = db.Column(db.JSON)
    distribute = db.Column(db.Boolean, default=False)
    def __repr__(self):
        return f"<OutputTable {self.unique_id}>"



def get_customer_data_from_db(customer_id):
    try:
        print("Connecting to the database...")
        conn = psycopg2.connect(
            dbname=dbname,
            user=os.getenv('DB_USER'),  
            password=os.getenv('DB_PASSWORD'),
            host=os.getenv('DB_HOST'),
            port=os.getenv('DB_PORT')
        )

        print("Database connection established.")

        cursor = conn.cursor(cursor_factory=DictCursor)

        print(f"Executing query for customer_id: {customer_id}")

        query_customer = """
        SELECT 
            c.custcd, 
            c.customername, 
            c.dob, 
            c.gender, 
            c.maritalstatus, 
            c.fathername, 
            c.mothername,
            c.phonenum, 
            c.alt_mobile_num, 
            c.email_id, 
            c.permanent_address, 
            c.occupation, 
            c.aadharno, 
            c.pan_number, 
            c.ckyc_status, 
            c.voterid, 
            c.drivers_license_number, 
            c.passport_number, 
            c.acctno, 
            c.acct_opendate, 
            c.acct_status, 
            c.card_type,
            o.query_number,        -- Add columns from output_table
            o.description, 
            o.date,
            c.acct_closedate,
            c.cust_type_code,
            c.branch_code,
            c.ifsc,
            c.card_number,
            c.cheque_no,
            c.cheque_issued_date,
            c.cheque_validity_date
FROM 
    customers c
LEFT JOIN 
    output_table o 
ON 
    c.custcd = o.customer_id   -- Adjust this join condition based on your schema
WHERE 
    c.custcd = %s;

        """
        cursor.execute(query_customer, (customer_id,))
        customer_data = cursor.fetchone()

        if customer_data:
            # Debug: Record fetched
            print("Customer data fetched:", customer_data)

            # Accessing data by column name
            customer_details = {
                'customer_id': customer_data['custcd'],
                'customername': customer_data['customername'],
                'dob': customer_data['dob'],
                'gender': customer_data['gender'],
                'maritalstatus': customer_data['maritalstatus'],
                'fathername': customer_data['fathername'],
                'mothername': customer_data['mothername'],
                'phonenum': customer_data['phonenum'],
                'alt_mobile_num': customer_data['alt_mobile_num'],
                'email_id': customer_data['email_id'],
                'permanent_address': customer_data['permanent_address'],
                'occupation': customer_data['occupation'],
                'aadharno': customer_data['aadharno'],
                'pan_number': customer_data['pan_number'],
                'ckyc_status': customer_data['ckyc_status'],
                'voterid': customer_data['voterid'],
                'drivers_license_number': customer_data['drivers_license_number'],
                'passport_number': customer_data['passport_number'],
                'acctno': customer_data['acctno'],
                'acct_opendate': customer_data['acct_opendate'],
                'acct_status': customer_data['acct_status'],
                'card_type': customer_data['card_type'],
                'query_number': customer_data['query_number'], 
                'description': customer_data['description'],
                'date': customer_data['date'],
                'acct_closedate': customer_data['acct_closedate'],
                'cust_type_code': customer_data['cust_type_code'],
                'branch_code': customer_data['branch_code'],
                'ifsc': customer_data['ifsc'],
                'card_number': customer_data['card_number'],
                'cheque_no': customer_data['cheque_no'],
                'cheque_issued_date': customer_data['cheque_issued_date'],
                'cheque_validity_date': customer_data['cheque_validity_date'],


            }
        else:
            print("No customer found with the given ID.")
            customer_details = {'error': 'Customer not found'}
        cursor.close()
        conn.close()

        return customer_details
    except Exception as e:
        print("Error fetching customer data:", e)
        return {'error': 'Database error'}



@app.route('/api/customer', methods=['POST'])
def customer():
    data = request.get_json() 
    print(data,"data")
    customer_id = data.get('customer_id')  
    print(customer_id)

    if not customer_id:
        return jsonify({'error': 'Customer ID not provided'}), 400
    customer_data = get_customer_data_from_db(customer_id)  
    return jsonify(customer_data)    


if __name__ == '__main__':
    app.run(host="0.0.0.0",debug=True, port=5003)
