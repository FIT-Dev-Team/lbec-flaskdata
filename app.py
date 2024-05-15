from flask import Flask, render_template, request, send_file
import pandas as pd
import sqlalchemy
import configparser
import os
import xlsxwriter
from queries import fetch_total_fw, fetch_fw_entries, fetch_cv_entries, fetch_blpr
from dotenv import load_dotenv
import logging
import tempfile

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

load_dotenv()

app = Flask(__name__)

# Load configuration# Function to create MySQL connection using SQLAlchemy
def create_connection():
    user = os.getenv('USERNAME')
    password = os.getenv('PASSWORD')
    host = os.getenv('HOST')
    database = os.getenv('DATABASE_NAME')
    connection_str = f"mysql+mysqlconnector://{user}:{password}@{host}/{database}"
    engine = sqlalchemy.create_engine(connection_str)
    return engine

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

    # Load configuration and create a database connection
    
    engine = create_connection()

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
    engine = create_connection() 
    try:
        fw = fetch_fw_entries(engine, company_name, start_date, end_date)
        cv = fetch_cv_entries(engine, company_name, start_date, end_date)
        if fw.empty and cv.empty:
            return "No data found for the given parameters."
       
        fw_columns = {'COMPANY_NAME': 'Property', 'KICHEN_NAME': 'Kitchen', 'OPERATION_DATE': 'Date', 'SHIFT_ID': 'Shift', 'IGD_CATEGORY_ID': 'Category', 'IGD_FOODTYPE_ID': 'Type of food', 'AMOUNT': 'Weight'}
        cv_columns = {'COMPANY_NAME': 'Property', 'KICHEN_NAME': 'Kitchen', 'OPERATION_DATE': 'Date', 'SHIFT_ID': 'Shift', 'AMOUNT': 'Covers'}
        fw2 = fw.rename(columns=fw_columns)
        cv2 = cv.rename(columns=cv_columns)
        sorted_fw = fw2[['Date','Property','Kitchen','Shift','Category','Type of food','Weight']]
        sorted_cv = cv2[['Date','Property','Kitchen','Shift','Covers']]
        temp_dir = tempfile.gettempdir()  # Gets the temporary directory
        file_path = os.path.join(temp_dir, f"{company_name}_FW&CV_Entries.xlsx")
        # Read dataframe into excel
        with pd.ExcelWriter(file_path, engine='xlsxwriter') as writer:
        # Write each DataFrame to a specific sheet
            sorted_fw.to_excel(writer, sheet_name='FW', index=False)
            sorted_cv.to_excel(writer, sheet_name='CV', index=False)
        # Return the file as a download
        return send_file(file_path, as_attachment=True)
    
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        return f"An error occurred: {str(e)}"

@app.route('/form_dcon')
def form_dcon():
    return render_template('form_dcon.html')

@app.route('/process_dcon', methods=['POST'])
def process_dcon():
    logger.info("Processing the dcon")
    #company_name = request.form['company_name']
    #end_date = request.form['end_date'
    engine = create_connection() 
    try:
        blpr = fetch_blpr(engine)
        if blpr.empty:
            return "No data found for the given parameters."
        
        temp_dir = tempfile.gettempdir()  # Gets the temporary directory
        file_path = os.path.join(temp_dir, f"Baseline_periods.csv")
        blpr.to_csv(file_path, index=False)
        
        return send_file(file_path, as_attachment=True)
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        return f"An error occurred: {str(e)}"
    

if __name__ == '__main__':
    # Note: Replace this with your production server run command, like using Gunicorn
    pass


