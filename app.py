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
from datetime import datetime

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
    database = os.getenv('database')
    
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

        # Validate date range
        if end_date < start_date:
            return "End date must be after start date."

        # Determine periods based on cutoff_date
        periods = []

        if end_date <= cutoff_date:
            # Entire period before cutoff_date (No CONS)
            periods.append({
                'start_date': start_date_str,
                'end_date': end_date_str,
                'CONS': False
            })
        elif start_date > cutoff_date:
            # Entire period after cutoff_date (Apply CONS)
            periods.append({
                'start_date': start_date_str,
                'end_date': end_date_str,
                'CONS': True
            })
        else:
            # Period spans cutoff_date; split into pre and post periods
            periods.append({
                'start_date': start_date_str,
                'end_date': cutoff_date.strftime('%Y-%m-%d'),
                'CONS': False
            })
            periods.append({
                'start_date': (cutoff_date + timedelta(days=1)).strftime('%Y-%m-%d'),
                'end_date': end_date_str,
                'CONS': True
            })

        # Initialize lists to collect dataframes
        monthly_dfs = []
        overall_dfs = []

        for period in periods:
            # Fetch monthly data
            month_df = DCON(
                engine=engine,
                company_name=company_name,
                start_date=period['start_date'],
                end_date=period['end_date'],
                grouping='monthly',
                CONS=period['CONS']
            )
            monthly_dfs.append(month_df)

            # Fetch overall data
            ov_df = DCON(
                engine=engine,
                company_name=company_name,
                start_date=period['start_date'],
                end_date=period['end_date'],
                grouping='overall',
                CONS=period['CONS']
            )
            overall_dfs.append(ov_df)

        # Concatenate dataframes
        monthly = pd.concat(monthly_dfs).sort_values(by=['COMPANY_NAME', 'KICHEN_NAME', 'OPERATION_DATE'])
        overall = pd.concat(overall_dfs)

        # Aggregate overall data
        overall = overall.groupby(['COMPANY_NAME', 'KICHEN_NAME']).agg({
            'COMP_SHIFTS': 'sum',
            'TOTAL_SHIFTS': 'sum',
            'CLOSED_SHIFTS': 'sum',
            'START_DATE': 'min',
            'END_DATE': 'max'
        }).reset_index()
        overall['CONSISTENCY'] = (overall['COMP_SHIFTS'] / (overall['TOTAL_SHIFTS'] - overall['CLOSED_SHIFTS'])).round(2)

        # Select relevant columns
        monthly = monthly[['COMPANY_NAME', 'KICHEN_NAME', 'OPERATION_DATE', 'CONSISTENCY', 'COMP_SHIFTS', 'TOTAL_SHIFTS', 'CLOSED_SHIFTS']]
        overall = overall[['COMPANY_NAME', 'KICHEN_NAME', 'CONSISTENCY', 'COMP_SHIFTS', 'TOTAL_SHIFTS', 'CLOSED_SHIFTS', 'START_DATE', 'END_DATE']]

        # Store the Excel file for download
        temp_dir = tempfile.gettempdir()
        file_path = os.path.join(temp_dir, f"{company_name}_dcon_data.xlsx")
        with pd.ExcelWriter(file_path, engine='xlsxwriter') as writer:
            monthly.to_excel(writer, sheet_name='Monthly Data', index=False)
            overall.to_excel(writer, sheet_name='Overall Data', index=False)

        # Convert dataframes to HTML for rendering
        month_table = monthly.to_html(classes='table table-striped table-bordered table-hover', index=False)
        overall_table = overall.to_html(classes='table table-striped table-bordered table-hover', index=False)

        # Return rendered template
        return render_template(
            'consistency.html',
            month_table=month_table,
            overall_table=overall_table,
            download_link=f"/download_excel/{os.path.basename(file_path)}"
        )

    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        return "An internal error occurred. Please try again later."


@app.route('/form_weekly')
def form_weekly():
    return render_template('form_weekly.html')

@app.route('/weekly_results', methods=['POST'])
def weekly_results():
    logger.info("Processing the wdcon")
    
    # Retrieve form data
    start_date = request.form['start_date']
    end_date = request.form['end_date']
    parent = request.form.get('parent_company')
    filter_option = request.form.get('calc_options')  # Matching form name
    
    # Set calculation method based on selection
    if filter_option == 'cons_false':
        CONS = False
        method = 'Pre-July2024'
    elif filter_option == 'cons_true':
        CONS = True
        method = 'Post-July2024'
    
    try:
        # Calculate weekly DCON
        week = DCON(engine=engine, start_date=start_date, end_date=end_date, grouping='weekly', CONS=CONS)
        logger.info("Calculated weekly dcon")
        
        today = datetime.now()
        # Group by parent company
        week = group_by_parent_company(week)
        week = week[week['LICENSE_EXPIRE_DATE'] >= today]
        week = week[['COMPANY_NAME', 'KICHEN_NAME', 'WEEK_START_DATE', 'CONSISTENCY','COMP_SHIFTS','TOTAL_SHIFTS','CLOSED_SHIFTS','PARENT_COMPANY']]
        # Filter by company
        constance = week[week['PARENT_COMPANY'].isin(['Constance'])]
        hyatt = week[week['PARENT_COMPANY'].isin(['Hyatt'])]
        marriott = week[~week['PARENT_COMPANY'].isin(['Constance', 'Hyatt'])]

        # Calculate average per group
        avg_constance = constance['CONSISTENCY'].mean()
        avg_hyatt = hyatt['CONSISTENCY'].mean()
        avg_marriott = marriott['CONSISTENCY'].mean()
        avg_per_parent = pd.DataFrame({
            'PARENT_COMPANY': ['Constance', 'Hyatt', 'Marriott & Others'],
            'CONSISTENCY': [avg_constance, avg_hyatt, avg_marriott]
        })
        
        # Store the Excel file for download
        temp_dir = tempfile.gettempdir()
        file_path = os.path.join(temp_dir, f"all_dcon_{method}.xlsx")
        
        # Save different sheets for each company
        with pd.ExcelWriter(file_path, engine='xlsxwriter') as writer:
            constance.to_excel(writer, sheet_name='Constance', index=False)
            hyatt.to_excel(writer, sheet_name='Hyatt', index=False)
            marriott.to_excel(writer, sheet_name='Marriott & Others', index=False)
            avg_per_parent.to_excel(writer, sheet_name='Average per Group', index=False)
        
        # Generate download link
        download_link = f"/download_excel/{os.path.basename(file_path)}"
        
        # Convert avg_per_parent DataFrame to HTML table
        avg_per_parent_table = avg_per_parent.to_html(classes='table table-striped table-bordered table-hover', index=False)
        
        # Render the appropriate table based on parent company selection
        if parent == 'constance':
            week_table = constance.to_html(classes='table table-striped table-bordered table-hover', index=False)
            week_table = week_table.replace('<table ', '<table id="week_table" ')
            return render_template(
                'weekly_dcon.html',
                week_table=week_table,
                avg_per_parent_table=avg_per_parent_table,
                download_link=download_link, parent_company=parent.capitalize())
        elif parent == 'hyatt':
            week_table = hyatt.to_html(classes='table table-striped table-bordered table-hover', index=False)
            week_table = week_table.replace('<table ', '<table id="week_table" ')
            return render_template(
                'weekly_dcon.html',
                week_table=week_table,
                avg_per_parent_table=avg_per_parent_table,
                download_link=download_link, parent_company=parent.capitalize())
        else:
            week_table = marriott.to_html(classes='table table-striped table-bordered table-hover', index=False)
            week_table = week_table.replace('<table ', '<table id="week_table" ')
            return render_template(
                'weekly_dcon.html',
                week_table=week_table,
                avg_per_parent_table=avg_per_parent_table,
                download_link=download_link, parent_company='Marriott & Others')

    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        return f"An error occurred: {str(e)}"

    
@app.route('/form_wdcon', methods=['GET', 'POST'])
def form_wdcon():
    return render_template('form_wdcon.html')


# Function for core logic of wdcon processing
def wdcon_logic(start_date, end_date, parent, CONS):
    logger.info("Processing wdcon logic")

    # Call the DCON function
    week = DCON(engine=engine, start_date=start_date, end_date=end_date, grouping='weekly', CONS=CONS)
    logger.info("Calculated weekly dcon")

    # Group by parent company
    week = group_by_parent_company(week)

    # Filter by company
    constance = week[week['PARENT_COMPANY'].isin(['Constance'])]
    hyatt = week[week['PARENT_COMPANY'].isin(['Hyatt'])]
    marriott = week[~week['PARENT_COMPANY'].isin(['Constance', 'Hyatt'])]

    # Calculate average per group
    avg_constance = constance['CONSISTENCY'].mean()
    avg_hyatt = hyatt['CONSISTENCY'].mean()
    avg_marriott = marriott['CONSISTENCY'].mean()
    avg_per_parent = pd.DataFrame({
        'PARENT_COMPANY': ['Constance', 'Hyatt', 'Marriott & Others'],
        'CONSISTENCY': [avg_constance, avg_hyatt, avg_marriott]
    })

    # Store Excel file for download
    temp_dir = tempfile.gettempdir()
    file_path = os.path.join(temp_dir, f"all_dcon_Pre_July2024.xlsx")  # Just an example

    with pd.ExcelWriter(file_path, engine='xlsxwriter') as writer:
        constance.to_excel(writer, sheet_name='Constance', index=False)
        hyatt.to_excel(writer, sheet_name='Hyatt', index=False)
        marriott.to_excel(writer, sheet_name='Marriott & Others', index=False)
        avg_per_parent.to_excel(writer, sheet_name='Average per Group', index=False)

    return file_path, avg_per_parent

# The Flask route remains the same
@app.route('/process_wdcon', methods=['POST'])
def process_wdcon():
    logger.info("Processing the wdcon route")

    # Get form data from request
    start_date = request.form['start_date']
    end_date = request.form['end_date']
    parent = request.form.get('parent_company')
    filter_option = request.form.get('calc_options')

    # Set calculation method based on selection
    CONS = filter_option == 'cons_true'

    # Call the logic function
    file_path, avg_per_parent = wdcon_logic(start_date, end_date, parent, CONS)

    # Handle further response logic (e.g., rendering or sending file)
    return send_file(file_path, as_attachment=True)


@app.route('/download_excel/<filename>')
def download_excel(filename):
    
    temp_dir = tempfile.gettempdir()
    file_path = os.path.join(temp_dir, filename)
    return send_file(file_path, as_attachment=True)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=10000)




        

