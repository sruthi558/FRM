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
dbname = os.getenv('DB_NAME')
user = os.getenv('DB_USER')
password = os.getenv('DB_PASSWORD')
host = os.getenv('DB_HOST')
port = os.getenv('DB_PORT')
db = SQLAlchemy(app)
jwt = JWTManager(app)
mail = Mail(app)

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
    frm_cell_head_scn_comments=db.Column(db.String(500))
    frm_cell_head_scn_date_updated=db.Column(db.DateTime)
    fraud_cat_report_rbi= db.Column(db.String(500))
    frm_cell_head_sendback_reason= db.Column(db.String(500))
    co_checker_sendback_reason= db.Column(db.String(500))
    co_maker_sendback_reason= db.Column(db.String(500))
    ro_checker_sendback_reason=db.Column(db.String(500))
    sendback_to=db.Column(db.String(500))
    submited_to= db.Column(db.String(500))
    def __repr__(self):
        return f"<OutputTable {self.unique_id}>"


@app.route('/upload_file', methods=['POST'])
def upload_file():
    print("inside of the function")
    if 'file' not in request.files:
        print("inside of the function")
        return jsonify({"message": "No file part"}), 400
    file = request.files['file']
    print("inside of the function")
    if file.filename == '':
        return jsonify({"message": "No selected file"}), 400
    try:
        print("inside of the function")
        # Extract table name from the filename
        table_name = file.filename.rsplit('.', 1)[0].lower()
        # Read the file into memory
        file_data = BytesIO(file.read())
        # Process the Excel file and insert data into the database
        process_excel_file(file_data, table_name)
        # query_number = 21
        execute_query_and_store_data()
        return jsonify({'message': 'File uploaded and data stored successfully!'}), 200
    except Exception as e:
        return jsonify({"message": f"Failed to upload file: {str(e)}"}), 500

# Process Excel file and insert into the target table
def process_excel_file(file_data, table_name):
    excel_data = pd.read_excel(file_data, sheet_name=None)
    # Combine all sheets into one DataFrame
    combined_data = pd.concat(excel_data.values(), ignore_index=True)
    print(f"Combined data:\n{combined_data.head()}")
    # Normalize column names
    combined_data.columns = [col.strip().lower() for col in combined_data.columns]
    if table_exists(table_name):
        print(f"Table {table_name} exists.")
        db_columns = get_table_columns(table_name)
        matching_columns = match_columns(combined_data.columns, db_columns)
        if matching_columns:
            print(f"Matching columns found. Inserting data into {table_name}.")
            insert_data_to_db(combined_data, table_name)
        else:
            print(f"No matching columns found for table {table_name}. Skipping.")
    else:
        print(f"Table {table_name} does not exist. Skipping.")
def insert_data_to_db(df, table_name):
    conn = get_db_connection()
    cursor = conn.cursor()
    # Get database columns for the table
    db_columns = get_table_columns(table_name)
    matching_columns = match_columns(df.columns, db_columns)
    if not matching_columns:
        print(f"No matching columns found for table {table_name}. Skipping.")
        return
    for _, row in df.iterrows():
        try:
            # Dynamically generate INSERT query
            columns_to_insert = ', '.join(matching_columns)
            placeholders = ', '.join(['%s'] * len(matching_columns))
            values = [row[col] for col in matching_columns]
            insert_query = f"INSERT INTO {table_name} ({columns_to_insert}) VALUES ({placeholders});"
            cursor.execute(insert_query, values)
            conn.commit()
        except Exception as e:
            print(f"Error inserting row {row}: {e}")
            conn.rollback()
    cursor.close()
    conn.close()
def execute_query_and_store_data():
    try:
        print("Starting the database operation...")
        connection = psycopg2.connect(
            dbname=dbname,
            user=os.getenv('DB_USER'),  
            password=os.getenv('DB_PASSWORD'),
            host=os.getenv('DB_HOST'),
            port=os.getenv('DB_PORT')
        )
        cursor = connection.cursor()
        print("Connected to the database.")

        query_execution_functions = [execute_queries_21, execute_queries_19]

        for query_func in query_execution_functions:
            # Fetch query, query number, and description
            query, query_number, description = query_func()

            # Execute the query
            print(f"Executing Query {query_number}: {description}")
            cursor.execute(query)
            results = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            print(f"Fetched {len(results)} rows from Query {query_number}.")

            # Check if the output_table exists
            cursor.execute("""SELECT to_regclass('output_table');""")
            table_exists = cursor.fetchone()[0]
            if not table_exists:
                # Create table if it doesn't exist
                create_table_query = f"""
                    CREATE TABLE IF NOT EXISTS output_table (
                        unique_id SERIAL PRIMARY KEY,
                        {', '.join([f'{col} VARCHAR' for col in columns])},
                        date TIMESTAMP,
                        query_number INT,
                        description TEXT,
                        comments_by_ro_maker VARCHAR(500),
                        date_updated_by_ro_maker TIMESTAMP,
                        comments_by_ro_checker VARCHAR(500),
                        date_updated_by_ro_checker TIMESTAMP,
                        comments_by_co_maker VARCHAR(500),
                        date_updated_by_co_maker TIMESTAMP,
                        co_maker_status VARCHAR(500),
                        co_maker_action VARCHAR(500),
                        co_maker_scn_status VARCHAR(500),
                        co_maker_scn_comments VARCHAR(500),
                        co_maker_scn_date_updated DATE,
                        comments_by_co_checker VARCHAR(500),
                        date_updated_by_co_checker TIMESTAMP,
                        co_checker_status VARCHAR(500),
                        co_checker_action VARCHAR(500),
                        co_checker_scn_action VARCHAR(500),
                        co_checker_scn_comments VARCHAR(500),
                        co_checker_scn_date_updated TIMESTAMP,
                        reply_body TEXT,
                        ro_maker_files JSONB,
                        ro_checker_files JSONB,
                        co_maker_files JSONB,
                        co_checker_files JSONB,
                        attachments JSONB,
                        distribute BOOLEAN DEFAULT FALSE,
                        frm_cell_head VARCHAR(500),
                        date_updated_by_frm_cell_head TIMESTAMP,
                        audit_comment VARCHAR(500),
                        date_updated_by_audit TIMESTAMP,
                        frm_cell_head_files JSONB,
                        audit_files JSONB,
                        frm_cell_head_scn_comments VARCHAR(500),
                        frm_cell_head_scn_date_updated TIMESTAMP,
                        fraud_cat_report_rbi VARCHAR(500),
                        frm_after_audit VARCHAR(500),
                        frm_after_audit_files JSONB,
                        frm_after_audit_date TIMESTAMP,
                        frm_cell_head_sendback_reason VARCHAR(500),
                        co_checker_sendback_reason VARCHAR(500),
                        co_maker_sendback_reason VARCHAR(500),
                        ro_checker_sendback_reason VARCHAR(500),
                        sendback_to VARCHAR(500),
                        submited_to VARCHAR(500)
                    );
                """
                cursor.execute(create_table_query)
                connection.commit()
                print("Table 'output_table' has been created.")
            else:
                # Check if any new columns are in the query result
                cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'output_table';")
                existing_columns = {row[0] for row in cursor.fetchall()}
                new_columns = [col for col in columns if col not in existing_columns]

                # Alter table to add new columns
                for new_col in new_columns:
                    alter_table_query = f"""
                        ALTER TABLE output_table ADD COLUMN IF NOT EXISTS {new_col} VARCHAR;
                    """
                    cursor.execute(alter_table_query)
                    connection.commit()
                    print(f"Column '{new_col}' added to 'output_table'.")

                # Add static columns if they don't exist
                static_columns = [
                    'query_number', 'description', 'comments_by_ro_maker', 'date_updated_by_ro_maker', 
                    'comments_by_ro_checker', 'date_updated_by_ro_checker', 'comments_by_co_maker', 
                    'date_updated_by_co_maker', 'co_maker_status', 'co_maker_action', 'co_maker_scn_status', 
                    'co_maker_scn_comments', 'co_maker_scn_date_updated', 'comments_by_co_checker', 
                    'date_updated_by_co_checker', 'co_checker_status', 'co_checker_action', 'co_checker_scn_action', 
                    'co_checker_scn_comments', 'co_checker_scn_date_updated', 'reply_body', 
                    'ro_maker_files', 'ro_checker_files', 'co_maker_files', 'co_checker_files', 'attachments', 
                    'distribute','frm_cell_head','date_updated_by_frm_cell_head','audit_comment', 'date_updated_by_audit','frm_cell_head_files','audit_files',
                    'frm_cell_head_scn_comments',
                    'frm_cell_head_scn_date_updated',
                    'fraud_cat_report_rbi','frm_after_audit','frm_after_audit_files','frm_after_audit_date','frm_cell_head_sendback_reason','co_checker_sendback_reason',
                    'co_maker_sendback_reason','ro_checker_sendback_reason','sendback_to','submited_to'
                ]
                for column in static_columns:
                    if column not in existing_columns:
                        alter_table_query = f"""
                            ALTER TABLE output_table ADD COLUMN {column} VARCHAR;
                        """
                        cursor.execute(alter_table_query)
                        connection.commit()
                        print(f"Static column '{column}' added to 'output_table'.")

            # Insert data into the output_table
            insert_query = f"""
                INSERT INTO output_table ({', '.join(columns)}, date, query_number, description)
                VALUES ({', '.join(['%s' for _ in columns])}, %s, %s, %s)
            """
            current_date = datetime.now()
            cursor.executemany(insert_query, [
                row + (current_date, query_number, description) for row in results
            ])
            connection.commit()
            print(f"Data from Query {query_number} has been successfully inserted into 'output_table'.")

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()
            print("Connection closed.")


def execute_queries_21():
    """
    Executes Query 21 and returns the result, query number, and description.
    """
    query = """
        SELECT
            t.*,
            c.customername,
            c.pan_number,
            c.aadharno,
            c.branch_name as branch,
            c.branch_region,
            c.branch_code,
            c.email_id
        FROM
            lc_issued AS t
        JOIN
            customers AS c ON t.customer_id = c.custcd
        WHERE
            lc_beneficiary_party <> trade_transaction_beneficiary_party
    """
    description = "Query for customers where the beneficiary party differs from the trade transaction beneficiary party."
    return query, 21, description

def execute_queries_19():
    """
    Executes Query 19 and returns the result, query number, and description.
    """
    query = """
                SELECT DISTINCT on (c.custcd)
            c.custcd AS customer_id,
            subquery.account_number,
            subquery.adhoc_loan_request_id,
            subquery.remarks,
            subquery.currency_code,
            subquery.adhoc_date_of_request,
            subquery.adhoc_date_of_sanction,
            subquery.purpose,
            subquery.loan_approval_status,
            c.pan_number,
            c.aadharno,
            c.customername,
            c.branch_name as branch,
            c.branch_code,
            c.branch_region,
            c.email_id
            FROM (
            SELECT DISTINCT
            a.*,
            LAG(a.adhoc_date_of_sanction) OVER (PARTITION BY a.customer_id ORDER BY a.adhoc_date_of_sanction) AS previous_sanction_date
            FROM
            adhoc AS a
            ) AS subquery
            JOIN customers AS c ON subquery.customer_id = c.custcd
            WHERE
            subquery.previous_sanction_date IS NOT NULL
            AND subquery.adhoc_date_of_request <= subquery.previous_sanction_date + INTERVAL '3 months'

            UNION

            SELECT DISTINCT on (c.custcd)
            c.custcd AS customer_id,
            a.account_number,
            a.adhoc_loan_request_id,
            a.remarks,
            a.currency_code,
            a.adhoc_date_of_request,
            a.adhoc_date_of_sanction,
            a.purpose,  
            a.loan_approval_status,
            c.pan_number,
            c.aadharno,
            c.customername,
            c.branch_name as branch,
            c.branch_code,
            c.branch_region,
            c.email_id



            FROM
            customer_loans AS cl
            JOIN
            adhoc AS a ON cl.customer_id = a.customer_id
            JOIN
            customers AS c ON cl.customer_id = c.custcd
            WHERE
            a.adhoc_date_of_request BETWEEN cl.date_of_sanction AND cl.date_of_sanction + INTERVAL '6 months'

            ORDER BY
            customer_id, adhoc_date_of_sanction;

    """
    
    description = "Query for customer loan approvals and adhoc sanctions with specified conditions."
    return query, 19, description


def get_db_connection():
    
    conn = psycopg2.connect(dbname=dbname,
            user=os.getenv('DB_USER'),  
            password=os.getenv('DB_PASSWORD'),
            host=os.getenv('DB_HOST'),
            port=os.getenv('DB_PORT'))
    return conn
def table_exists(table_name):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = %s);", (table_name,))
    exists = cursor.fetchone()[0]
    cursor.close()
    conn.close()
    return exists
# Get table columns
def get_table_columns(table_name):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = %s;", (table_name,))
    columns = [col[0] for col in cursor.fetchall()]
    cursor.close()
    conn.close()
    return columns
# Match columns between Excel and database
def match_columns(excel_columns, db_columns):
    excel_columns = [col.strip().lower() for col in excel_columns]
    db_columns = [col.strip().lower() for col in db_columns]
    return [col for col in excel_columns if col in db_columns]
# Insert data into the database
if __name__ == '__main__':
    app.run(host="0.0.0.0",debug=True, port=5004)
