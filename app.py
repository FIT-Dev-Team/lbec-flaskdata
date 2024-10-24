from flask import Flask, render_template, request, send_file
import pandas as pd
import sqlalchemy
import configparser
import os
import xlsxwriter
from queries import *
from dotenv import load_dotenv
import logging
import tempfile
import numpy as np
import matplotlib.pyplot as plt
import matplotlib
import zipfile
from werkzeug.utils import secure_filename
import urllib.parse
from calculations import *
from graph import *

logging.getLogger('matplotlib.category').setLevel(logging.WARNING)


matplotlib.use('Agg')

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

load_dotenv()

app = Flask(__name__)

# Load configuration# Function to create MySQL connection using SQLAlchemy
def create_connection():
    user = os.getenv('user')
    password = os.getenv('password')
    host = os.getenv('host')
    database = urllib.parse.quote_plus(os.getenv('database'))
    
    # Encode the password to handle special characters
    encoded_password = urllib.parse.quote_plus(password)
    
    # Create the connection string with the encoded password
    connection_str = f"mysql+mysqlconnector://{user}:{encoded_password}@{host}/{database}"
    engine = sqlalchemy.create_engine(connection_str)
    return engine

engine=create_connection()

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/form_total_fw')
def form_total_fw():
    return render_template('form_total_fw.html')

@app.route('/process_total_fw', methods=['POST'])
def process_total_fw():
    logger.info("Processing the total FW")
    company_name = request.form['company_name']
    start_date = request.form['start_date']
    end_date = request.form['end_date']

    try:
        # Fetch data
        fw = fetch_total_fw(engine, company_name, start_date, end_date)

        if fw.empty:
            return "No data found for the given parameters."

        # Process data into a pivot table
        np_fw = fw[fw['IGD_CATEGORY_ID'] != 'PLATE']
        pivot = np_fw.pivot_table(index=['COMPANY_NAME', 'KICHEN_NAME'], columns='IGD_FOODTYPE_ID', values='AMOUNT', aggfunc='sum').reset_index()
        pivot['START'] = np_fw['OPERATION_DATE'].min()
        pivot['END'] = np_fw['OPERATION_DATE'].max()

        # Select columns containing float values for summation
        float_columns = pivot.select_dtypes(include=['float64', 'int64']).columns

        # Calculate the total sum horizontally (ignores NaN values)
        pivot['TOTAL'] = pivot[float_columns].sum(axis=1)

        # Find the insertion index for the 'TOTAL' column (before 'START' and 'END')
        start_index = pivot.columns.get_loc('START')

        # Reorder columns to place 'TOTAL' before 'START'
        columns_order = list(pivot.columns)
        columns_order.remove('TOTAL')
        columns_order.insert(start_index, 'TOTAL')

        # Reassign the reordered columns
        pivot = pivot[columns_order]
        # Create the file path for the CSV
        temp_dir = tempfile.gettempdir()  # Gets the temporary directory
        file_path = os.path.join(temp_dir, f"{company_name}_Total_FW.csv")

        pivot.to_csv(file_path, index=False)
        return send_file(file_path, as_attachment=True)


    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        return f"An error occurred: {str(e)}"
    
@app.route('/form_entries')
def form_entries():
    return render_template('form_entries.html')

@app.route('/process_entries', methods=['POST'])
def process_entries():
    logger.info("Processing the Entries")
    company_name = request.form['company_name']
    start_date = request.form['start_date']
    end_date = request.form['end_date']
    try:
        fw = fetch_fw_entries(engine, company_name, start_date, end_date)
        cv = fetch_cv_entries(engine, company_name, start_date, end_date)
        if fw.empty and cv.empty:
            return "No data found for the given parameters."
       
        fw_columns = {'COMPANY_NAME': 'Property', 'KICHEN_NAME': 'Kitchen', 'OPERATION_DATE': 'Date', 'SHIFT_ID': 'Shift', 'IGD_CATEGORY_ID': 'Category', 'IGD_FOODTYPE_ID': 'Type of food', 'AMOUNT': 'Weight'}
        cv_columns = {'COMPANY_NAME': 'Property', 'KICHEN_NAME': 'Kitchen', 'OPERATION_DATE': 'Date', 'SHIFT_ID': 'Shift', 'AMOUNT': 'Covers'}
        fw2 = fw.rename(columns=fw_columns)
        cv2 = cv.rename(columns=cv_columns)
        sorted_fw = fw2[['Date','Property','Kitchen','Shift','Category','Weight','Type of food']]
        sorted_cv = cv2[['Date','Property','Kitchen','Shift','Covers']]
        replacement = {'DAIRY':'Dairy/Egg','STAPLE_FOOD':'Staple food'}
        sorted_fw.loc[:,'Type of food'] = sorted_fw['Type of food'].replace(replacement)

        # Add how many entries per kitchen 
        entry_counts = sorted_fw.groupby(['Property','Kitchen']).size().reset_index(name='Count')        

        temp_dir = tempfile.gettempdir()  # Gets the temporary directory
        file_path = os.path.join(temp_dir, f"{company_name}_FW&CV_Entries.xlsx")
        # Read dataframe into excel
        with pd.ExcelWriter(file_path, engine='xlsxwriter') as writer:
        # Write each DataFrame to a specific sheet
            sorted_fw.to_excel(writer, sheet_name='FW', index=False)
            sorted_cv.to_excel(writer, sheet_name='CV', index=False)
            entry_counts.to_excel(writer, sheet_name='FW Entry Counts', index=False)
        # Return the file as a download
        return send_file(file_path, as_attachment=True)
    
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        return f"An error occurred: {str(e)}"

@app.route('/form_dcon')
def form_dcon():
    return render_template('form_dcon.html')

from datetime import datetime

@app.route('/process_dcon', methods=['GET', 'POST'])
def process_dcon():
    company_name = request.form.get('company_name')
    start_date_str = request.form.get('start_date')
    end_date_str = request.form.get('end_date')

    # Validate input fields
    if not company_name or not start_date_str or not end_date_str:
        return "All fields (company_name, start_date, end_date) are required."

    try:
        # Convert date strings to datetime objects
        start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
        end_date = datetime.strptime(end_date_str, '%Y-%m-%d')
        cutoff_date = datetime(2024, 6, 30)

        if start_date <= cutoff_date and end_date <= cutoff_date:
            # No CONS
            month = DCON(company_name=company_name, start_date=start_date_str, end_date=end_date_str, grouping='monthly', CONS=False)
            ov = DCON(company_name=company_name, start_date=start_date_str, end_date=end_date_str, grouping='overall', CONS=False)
        elif start_date > cutoff_date:
            # Apply CONS
            month = DCON(company_name=company_name, start_date=start_date_str, end_date=end_date_str, grouping='monthly', CONS=True)
            ov = DCON(company_name=company_name, start_date=start_date_str, end_date=end_date_str, grouping='overall', CONS=True)
        elif start_date <= cutoff_date and end_date > cutoff_date:
            # Split into pre- and post-cutoff date
            start_date_pre = start_date_str
            end_date_pre = '2024-06-30'
            start_date_post = '2024-07-01'

            # Pre-cutoff (No CONS)
            month_pre = DCON(company_name=company_name, start_date=start_date_pre, end_date=end_date_pre, grouping='monthly', CONS=False)
            # Post-cutoff (CONS)
            month_post = DCON(company_name=company_name, start_date=start_date_post, end_date=end_date_str, grouping='monthly', CONS=True)

            # Merge pre and post data
            month = pd.concat([month_pre, month_post])
            month = month.sort_values(by=['COMPANY_NAME', 'KICHEN_NAME', 'OPERATION_DATE'])

            # Overall calculation split similarly
            ov_pre = DCON(company_name=company_name, start_date=start_date_pre, end_date=end_date_pre, grouping='overall', CONS=False)
            ov_post = DCON(company_name=company_name, start_date=start_date_post, end_date=end_date_str, grouping='overall', CONS=True)
            ov = pd.concat([ov_pre, ov_post])

            ov = ov.groupby(['COMPANY_NAME', 'KICHEN_NAME']).agg({
                'COMP_SHIFTS': 'sum', 
                'TOTAL_SHIFTS': 'sum', 
                'CLOSED_SHIFTS': 'sum',
                'START_DATE': 'min',
                'END_DATE': 'max'
            }).reset_index()

            ov['CONSISTENCY'] = (ov['COMP_SHIFTS'] / (ov['TOTAL_SHIFTS'] - ov['CLOSED_SHIFTS'])).round(2)
        else:
            return "No data found for the given parameters."

        # Handling the results and rendering tables
        monthly = month[['COMPANY_NAME', 'KICHEN_NAME', 'OPERATION_DATE', 'CONSISTENCY', 'COMP_SHIFTS', 'TOTAL_SHIFTS', 'CLOSED_SHIFTS']]
        ov2 = ov[['COMPANY_NAME', 'KICHEN_NAME', 'CONSISTENCY','COMP_SHIFTS', 'TOTAL_SHIFTS', 'CLOSED_SHIFTS', 'START_DATE', 'END_DATE']]

        if month.empty:
            return "No data found for the given parameters."

        # Generate and store charts
        charts = plot_graph(month)

        # Store the Excel file for download
        temp_dir = tempfile.gettempdir()
        file_path = os.path.join(temp_dir, f"{company_name}_dcon_data.xlsx")
        with pd.ExcelWriter(file_path, engine='xlsxwriter') as writer:
            monthly.to_excel(writer, sheet_name='Monthly Data', index=False)
            ov2.to_excel(writer, sheet_name='Overall Data', index=True)

        # Convert dataframes to HTML for rendering
        month_table = monthly.to_html(classes='table table-striped table-bordered table-hover', index=False)
        overall_table = ov2.to_html(classes='table table-striped table-bordered table-hover', index=True)

        # Return rendered template
        return render_template(
            'consistency.html',
            month_table=month_table,
            overall_table=overall_table,
            charts_html=charts,
            download_link=f"/download_excel/{os.path.basename(file_path)}"
        )
    
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        return f"An error occurred: {str(e)}"

    
@app.route('/download_excel/<filename>')
def download_excel(filename):
    temp_dir = tempfile.gettempdir()
    file_path = os.path.join(temp_dir, filename)
    return send_file(file_path, as_attachment=True)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, debug=True)




        

