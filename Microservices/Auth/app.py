#microservices/Auth/app.py
from flask import Flask, jsonify, request
from flask_jwt_extended import (JWTManager, create_access_token, decode_token,jwt_required, get_jwt_identity)
from flask_sqlalchemy import SQLAlchemy
from flask_cors import CORS
from flask_bcrypt import Bcrypt
from flask_mail import Mail, Message
import random
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from sqlalchemy import text
import smtplib
import os
from dotenv import load_dotenv

load_dotenv()
app = Flask(__name__)
CORS(app)

app.config['JWT_SECRET_KEY'] = 'your_jwt_secret_key'  
app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://postgres:12345@localhost:5432/FRM'
app.config['MAIL_SERVER'] = 'smtp.gmail.com'
app.config['MAIL_PORT'] = 587
app.config['MAIL_USE_TLS'] = True
app.config['MAIL_USERNAME'] = 'durga@corundumtechnologies.com'
app.config['MAIL_PASSWORD'] = 'yaviezhynflcygvs'
jwt = JWTManager(app)
db = SQLAlchemy(app)
bcrypt = Bcrypt(app)
mail = Mail(app) 
##admin creation
##UserDetails Table
class UserDetails(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    emp_id = db.Column(db.String(50), unique=True)
    username = db.Column(db.String(80), unique=True, nullable=False)
    email_id = db.Column(db.String(120), unique=True, nullable=False)
    mobile_no = db.Column(db.String(15))
    address = db.Column(db.String(200))
    role = db.Column(db.String(50))
    status = db.Column(db.String(50))
    telephone_no = db.Column(db.String(15))
    branch = db.Column(db.String(50))
    branch_code = db.Column(db.String(15))
    branch_region = db.Column(db.String(15))
    is_admin = db.Column(db.Boolean, default=False)
    password = db.Column(db.String(200)) 
    code=db.Column(db.Integer,nullable=True)
    
def create_admin_user():
    if not UserDetails.query.filter_by(is_admin=True).first():
        hashed_password = bcrypt.generate_password_hash('adminpassword').decode('utf-8')
        admin_user = UserDetails(
            emp_id='admin001',
            username='admin',
            email_id='admin@example.com',
            mobile_no='1234567890',
            address='Admin Address',
            role='Admin',
            status='Active',
            telephone_no='9876543210',
            branch='Main',
            password=hashed_password,  
            is_admin=True,
       )
        db.session.add(admin_user)
        db.session.commit()
with app.app_context():
    db.create_all()
    create_admin_user()
##Login
@app.route('/', methods=['POST'])
def login_user():
    data = request.get_json()
    email = data.get('email')
    password = data.get('password')
    user = UserDetails.query.filter_by(email_id=email).first()
    if user and bcrypt.check_password_hash(user.password, password):
        access_token = create_access_token(identity=user.id)
        # Check if the 'code' column exists in the 'user_details' table
        check_column_query = text("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'user_details' AND column_name = 'code';
        """)
        result = db.session.execute(check_column_query)
        # If the 'code' column doesn't exist, add it
        if not result.fetchone():
            # Add the 'code' column dynamically
            add_column_query = text("""
                ALTER TABLE user_details ADD COLUMN code INTEGER;
            """)
            db.session.execute(add_column_query)
            db.session.commit()
        # Generate a 6-digit verification code
        verification_code = str(random.randint(100000, 999999))
        # Store the verification code in the database
        user.code = verification_code
        db.session.commit()  # Commit the transaction to save the code in the database
        # Send the verification code to the user's email
        send_verification_code(email, verification_code)
        # if user.email_id == 'admin@example.com':
        #     return jsonify({"message": "Login Successful", "access_token": access_token, "role": "admin"}), 200
        return jsonify({
                "message": "Verification code sent to your email.",
                "access_token": access_token,
                "role": user.role
            }), 200
            # return jsonify({"message": "Login Successful", "access_token": access_token, "role": user.role}), 200
    return jsonify({"message": "Invalid credentials"}), 401

def send_verification_code(email, code):
    # Create a MIME message
    msg = MIMEMultipart()
    msg['From'] = app.config['MAIL_USERNAME']  # Use the configured sender email
    msg['To'] = email
    msg['Subject'] = 'Your Verification Code'
    # Body of the email
    body = f'Your verification code is: {code}'
    msg.attach(MIMEText(body, 'plain'))
    print('msg:',msg)
    # Send the email using the configured email account
    with smtplib.SMTP('smtp.gmail.com', 587) as server:
        server.starttls()
        server.login(app.config['MAIL_USERNAME'], app.config['MAIL_PASSWORD'])  # Use configured credentials
        text = msg.as_string()
        server.sendmail(app.config['MAIL_USERNAME'], email, text)  # Send email from configured address
##add user
@app.route('/add_user', methods=['POST'])
@jwt_required()
def add_user():
    print("here")
    current_user = get_jwt_identity()
    print(current_user)
    user = UserDetails.query.filter_by(id=current_user).first()
    if not user or not user.is_admin:
        return jsonify({"message": "You do not have permission to add users"}), 403
    
    data = request.get_json()
    emp_id = data.get('emp_id')
    username = data.get('username')
    email_id = data.get('email_id')
    mobile_no = data.get('mobile_no')
    address = data.get('address')
    role = data.get('role')
    status = data.get('status')
    telephone_no = data.get('telephone_no')
    branch = data.get('branch')
    branch_code = data.get('branchcode')
    branch_region = data.get('branchregion')

    print(branch)

    if role in ["Officer1", "FRM Cell Head", "Audit", "CO Maker", "CO Checker"]:
        existing_user_with_role = UserDetails.query.filter_by(role=role).first()
        if existing_user_with_role:
            return jsonify({"message": f"The role '{role}' already exists."}), 400

    if UserDetails.query.filter_by(username=username).first():
        return jsonify({"message": "Username already exists"}), 400
    if UserDetails.query.filter_by(email_id=email_id).first():
        return jsonify({"message": "Email already exists"}), 400

    new_user = UserDetails(
        emp_id=emp_id,
        username=username,
        email_id=email_id,
        mobile_no=mobile_no,
        address=address,
        role=role,
        status=status,
        telephone_no=telephone_no,
        branch=branch,
        branch_code=branch_code,
        branch_region=branch_region,
        is_admin=False
    )
    db.session.add(new_user)
    db.session.commit()

    reset_token = create_access_token(identity=new_user.id)
    reset_link = f"http://localhost:3000/create-password/{reset_token}"
    try:
        msg = Message(
            "Create Your Password",
            sender=app.config['MAIL_USERNAME'],
            recipients=[email_id]
        )
        msg.body = f"Hi {username}, click the link to create your password: {reset_link}"
        mail.send(msg)
        return jsonify({"message": "User added successfully, and email with password creation link sent."}), 201
    except Exception as e:
        print(f"Error sending email: {e}")
        return jsonify({"message": "User added successfully, but failed to send email."}), 201

@app.route('/forgot-password', methods=['POST'])
def forgot_password():
    data = request.get_json()
    print("data")
    email = data.get('email')
    user = UserDetails.query.filter_by(email_id=email).first()
    if not user:
        return jsonify({"message": "Email not found"}), 404
    reset_token = create_access_token(identity=user.id)
    try:
        reset_link = f"http://localhost:3000/create-password/{reset_token}"  
        msg = Message(
            "Password Reset Request",
            sender=app.config['MAIL_USERNAME'],
            recipients=[email]
        )
        msg.body = f"Hi {user.username}, click the link to reset your password: {reset_link}"
        mail.send(msg)
        return jsonify({"message": "Password reset link sent to your email"}), 200
    except Exception as e:
        print(f"Error sending email: {e}")
        return jsonify({"message": "Failed to send reset link"}), 500
    
@app.route('/reset-password', methods=['POST'])
def reset_password():
    data = request.get_json()
    print(data,"123456")
    reset_token = data.get('reset_token')
    print(reset_token)
    new_password = data.get('password')
    try:
        decoded_token = decode_token(reset_token)
        user_id = decoded_token['sub']
        user = UserDetails.query.filter_by(id=user_id).first()
        if not user:
            return jsonify({"message": "Invalid token or user not found"}), 404
        hashed_password = bcrypt.generate_password_hash(new_password).decode('utf-8')
        user.password = hashed_password
        db.session.commit()
        return jsonify({"message": "Your Password Changed successfully"}), 200
    except Exception as e:
        print(f"Error resetting password: {e}")
        return jsonify({"message": "Failed to reset password"}), 500


@app.route('/get_user', methods=['GET'])
@jwt_required()
def get_logged_in_user():
    user_id = get_jwt_identity()
    user = UserDetails.query.get(user_id)
    print(user,"12345")
    if not user:
        return jsonify({"message": "User not found"}), 404
    return jsonify({"user": {
        "id": user.id,
        "username": user.username,
        "email_id": user.email_id,
        "role": user.role
    }}), 200

@app.route('/view_users', methods=['GET'])
@jwt_required()
def view_users():
    current_user = get_jwt_identity()
    print("!23")
    user = UserDetails.query.filter_by(id=current_user).first()
    if not user or not user.is_admin:
        return jsonify({"message": "You do not have permission to view users"}), 403
    # users = UserDetails.query.all()
    users = UserDetails.query.filter(UserDetails.role != 'Admin').all()
    users_data = [
        {
            'id': u.id,
            'emp_id': u.emp_id,
            'username': u.username,
            'email_id': u.email_id,
            'mobile_no': u.mobile_no,
            'role': u.role,
            'status': u.status,
            'telephone_no': u.telephone_no,
            'branch':u.branch,
            'branch_code':u.branch_code,
            'branch_region':u.branch_region,
            'address':u.address
        }
        for u in users
    ]
    return jsonify({'users': users_data}), 200


@app.route('/get_user/<int:user_id>', methods=['GET'])
@jwt_required()
def get_user(user_id):
    current_user = get_jwt_identity() 
    user = UserDetails.query.filter_by(id=user_id).first() 
    print("user",user) 
    if not user:
        return jsonify({'message': 'User not found'}), 404
    return jsonify({
        'user': {
            'id': user.id,
            'emp_id': user.emp_id,
            'username': user.username,
            'email_id': user.email_id,
            'mobile_no': user.mobile_no,
            'role': user.role,
            'status': user.status,
            'address':user.address,
            'telephone_no':user.telephone_no,
            'branch':user.branch
        }

    }), 200
@app.route('/update_user_details/<emp_id>', methods=['PUT'])
def update_user_details(emp_id):
    data = request.get_json()

    # Find the user by emp_id
    user = UserDetails.query.filter_by(emp_id=emp_id).first()
    if not user:
        return jsonify({"message": "User not found"}), 404

    # Track changes
    changes = {
        "username": data.get('username', user.username),
        "email_id": data.get('email_id', user.email_id),
        "mobile_no": data.get('mobile_no', user.mobile_no),
        "role": data.get('role', user.role),
        "status": data.get('status', user.status),
        "address": data.get('address', user.address),
        "telephone_no": data.get('telephone_no', user.telephone_no),
        "branch": data.get('branch', user.branch),
        "branch_code": data.get('branch_code', user.branch_code),
        "branch_region": data.get('branch_region', user.branch_region),
    }

    if all(getattr(user, field) == value for field, value in changes.items()):
        return jsonify({"message": "No new data to update"}), 400

    # Update user fields
    user.username = changes["username"]
    user.email_id = changes["email_id"]
    user.mobile_no = changes["mobile_no"]
    user.role = changes["role"]
    user.status = changes["status"]
    user.address = changes["address"]
    user.telephone_no = changes["telephone_no"]
    user.branch = changes["branch"]
    user.branch_code = changes["branch_code"]
    user.branch_region = changes["branch_region"]

    try:
        db.session.commit()
        return jsonify({"message": "User details updated successfully"}), 200
    except Exception as e:
        db.session.rollback()
        return jsonify({"message": "Error updating user details", "error": str(e)}), 500

@app.route('/verify', methods=['POST'])
def verify_code():
    data = request.get_json()
    email = data.get('email')
    entered_code = data.get('code')
    # Fetch user based on email
    user = UserDetails.query.filter_by(email_id=email).first()
    if user:
        # Convert both the stored code and entered code to strings
        stored_code = str(user.code).strip()  # Ensure it's a string and stripped of spaces
        entered_code = str(entered_code).strip()  # Ensure it's a string and stripped of spaces
        # Log both values to compare them
        print(f"Stored Code: '{stored_code}'")
        print(f"Entered Code: '{entered_code}'")
        # Compare the two codes
        if stored_code == entered_code:
            print('@@@@@@@@@@@')  # If codes match, verification is successful
            return jsonify({"message": "Verification successful"}), 200
        else:
            return jsonify({"message": "Invalid verification code"}), 400
    else:
        return jsonify({"message": "User not found"}), 404
if __name__ == '__main__':
    app.run(host="0.0.0.0",debug=True, port=5001)  
