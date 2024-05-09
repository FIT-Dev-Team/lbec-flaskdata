from flask import Flask, render_template, request, send_file
import pandas as pd
import sqlalchemy
import configparser
import os
import xlsxwriter
from queries import fetch_total_fw, fetch_fw_entries, fetch_cv_entries

app = Flask(__name__)

# Load configuration
def load_configuration(file_path='config.ini'):
    config_parser = configparser.ConfigParser()
    config_parser.read(file_path)
    db_config = dict(config_parser['mysql'])
    return db_config

# Create MySQL connection using SQLAlchemy
def create_connection(db_config):
    connection_str = f"mysql+mysqlconnector://{db_config['user']}:{db_config['password']}@{db_config['host']}/{db_config['database']}"
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
    company_name = request.form['company_name']
    start_date = request.form['start_date']
    end_date = request.form['end_date']

    # Load configuration and create a database connection
    config = load_configuration()
    engine = create_connection(config)

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
        # Create the file path for the CSV
        home_dir = os.path.expanduser('~')
        file_path = os.path.join(home_dir, 'Documents', f"{company_name}_Total_FW.csv")

        # Write the CSV to the specified file path
        pivot.to_csv(file_path, index=False)

        # Return the file as a download
        return send_file(file_path, as_attachment=True)

    except Exception as e:
        return f"An error occurred: {str(e)}"
    
@app.route('/form_entries')
def form_entries():
    return render_template('form_entries.html')

@app.route('/process_entries', methods=['POST'])
def process_entries():
    company_name = request.form['company_name']
    start_date = request.form['start_date']
    end_date = request.form['end_date']
    config = load_configuration()
    engine = create_connection(config)
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
        home_dir = os.path.expanduser('~')
        file_path = os.path.join(home_dir, 'Documents', f"{company_name}_FW&CV_entries.xlsx")
        # Read dataframe into excel
        with pd.ExcelWriter(file_path, engine='xlsxwriter') as writer:
        # Write each DataFrame to a specific sheet
            sorted_fw.to_excel(writer, sheet_name='FW', index=False)
            sorted_cv.to_excel(writer, sheet_name='CV', index=False)
        # Return the file as a download
        return send_file(file_path, as_attachment=True)
    
    except Exception as e:
        return f"An error occurred: {str(e)}"

if __name__ == '__main__':
    app.run(debug=True)

