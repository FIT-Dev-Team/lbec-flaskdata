from flask import Flask, json, jsonify, render_template, request, session, redirect, url_for, send_file, make_response
from flask_scss import Scss
from flask_sqlalchemy import SQLAlchemy
import mysql.connector
import os
from datetime import timedelta
from dotenv import load_dotenv


# Load environment variables from the .env file
load_dotenv()

# Access environment variables
USERNAME = os.getenv('USERNAME')
PASSWORD = os.getenv('PASSWORD')
HOST = os.getenv('HOST')
DATABASE_NAME = os.getenv('DATABASE_NAME')
# Retrieve connection information from environment variables
LOGIN_USERNAME = os.getenv('LOGIN_USERNAME')
LOGIN_PASSWORD = os.getenv('LOGIN_PASSWORD')

app = Flask(__name__)
Scss(app)

app.secret_key = 'lightblue_session'

app.config.update(
    SESSION_COOKIE_SECURE=True,
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE='Lax',
    PERMANENT_SESSION_LIFETIME=timedelta(hours=6)  # One-hour session lifetime
)

@app.route('/login', methods=['GET', 'POST']) # Login Process Verification
def login():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']

        if username == LOGIN_USERNAME and password == LOGIN_PASSWORD:
            session['user_id'] = 'identifiant_unique_utilisateur'
            return redirect(url_for('total_fw'))  # Redirect to total_fw
        else:
            return "Échec de la connexion", 401
    else:
        return render_template('login.html')

def is_logged_in():
    return 'user_id' in session
 

@app.route('/')
def index():
    if not is_logged_in():
        return redirect(url_for('login'))

@app.route('/total_fw')
def total_fw():
    return render_template("total_fw.html")
 

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001, debug=True)