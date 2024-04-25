from flask import Flask, json, jsonify, render_template, request, session, redirect, url_for, send_file
from flask_scss import Scss
from flask_sqlalchemy import SQLAlchemy
import mysql.connector
import os

from flask import Flask
 

app = Flask(__name__)
 

@app.route('/')
def index():
    return render_template("index.html")
 

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001, debug=True)