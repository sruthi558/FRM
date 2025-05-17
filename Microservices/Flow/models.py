# models.py
from flask_sqlalchemy import SQLAlchemy
db = SQLAlchemy()
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
    def __repr__(self):
        return f'<UserDetails {self.username}>'

class OutputTable(db.Model):
    __tablename__ = 'output_table'  
    unique_id = db.Column(db.Integer, primary_key=True)  
    comments_by_ro_maker = db.Column(db.String(500))
    comments_by_ro_checker = db.Column(db.String(500)) 
    comments_by_co_maker = db.Column(db.String(500)) 
    comments_by_co_checker = db.Column(db.String(500))
    def __repr__(self):
        return f"<OutputTable {self.unique_id}>"
