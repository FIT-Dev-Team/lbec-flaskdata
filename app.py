from flask import Flask, json, jsonify, render_template, request, session, redirect, url_for, send_file
import mysql.connector
from dotenv import load_dotenv
import os

from flask import Flask
 
# Flask constructor takes the name of 
# current module (__name__) as argument.
app = Flask(__name__)
 
# The route() function of the Flask class is a decorator, 
# which tells the application which URL should call 
# the associated function.
@app.route('/')
# ‘/’ URL is bound with hello_world() function.
def index():
    return render_template('index.html')
 
# main driver function
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001, debug=True)