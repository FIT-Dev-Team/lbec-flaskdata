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

@app.route('/process_dcon', methods=['POST'])
def process_dcon():
    logger.info("Processing the dcon")
    company_name = request.form['company_name']
    end_date = request.form['end_date']
    engine = create_connection() 
    start_date = '2021-03-12'
    try:
        # Fetch data
        fwcv = fetch_fwcv(engine, company_name, end_date)
        if fwcv.empty:
            return "No data found for the given parameters (fwcv)."
        logger.info("Fetched fwcv data")

        firstdate = fetch_first_date(engine, company_name)
        if firstdate.empty:
            return "No data found for the given parameters (firstdate)."
        logger.info("Fetched firstdate data")

        opening_shifts = fetch_opening_shifts(engine, company_name)
        if opening_shifts.empty:
            return "No data found for the given parameters (opening_shifts)."
        logger.info("Fetched opening_shifts data")

        closed = fetch_closed_shifts(engine, company_name, end_date)
        
        logger.info("Fetched closed_shifts data")

        closed['CLOSE_DATE'] = pd.to_datetime(closed['CLOSE_DATE'])
        # Process data
        fwcv_old = fwcv.dropna()
        sum_fwcv_old = fwcv_old.groupby(['COMPANY_NAME', 'KICHEN_NAME', 'OPERATION_DATE', 'SHIFT_ID']).agg(
            FW=('FW', 'sum')
        ).reset_index()
        sum_fwcv_old['CV'] = fwcv_old.drop_duplicates(
            subset=['SHIFT_ID', 'OPERATION_DATE', 'KICHEN_NAME', 'COMPANY_NAME']
        ).groupby(['COMPANY_NAME', 'KICHEN_NAME', 'OPERATION_DATE', 'SHIFT_ID']).agg(
            CV=('CV', 'sum')
        ).reset_index()['CV']

        sum_fwcv_old['YEAR'] = pd.to_datetime(sum_fwcv_old['OPERATION_DATE']).dt.year
        sum_fwcv_old['MONTH'] = pd.to_datetime(sum_fwcv_old['OPERATION_DATE']).dt.month

        fwcv_comp2 = sum_fwcv_old.merge(firstdate, on=['COMPANY_NAME', 'KICHEN_NAME'], how='left')
        fwcv_comp2 = fwcv_comp2[fwcv_comp2['OPERATION_DATE'] > fwcv_comp2['FirstDate']]
        fwcv_comp2 = fwcv_comp2.drop(['FirstDate', 'COUNTRY_CODE'], axis=1)
        comp_set_per_month_old = fwcv_comp2.groupby(['COMPANY_NAME', 'KICHEN_NAME', 'YEAR', 'MONTH']).agg(
            COMP_SHIFTS_OLD=('SHIFT_ID', 'count')
        ).reset_index()
        comp_set_per_month_old['COMP_SHIFTS_OLD'] = comp_set_per_month_old['COMP_SHIFTS_OLD'].fillna(0)
        def get_dates_for_day(day_of_week, start_date, end_date):
        # Create a date range for the period of interest
            all_dates = pd.date_range(start=start_date, end=end_date)
            # Filter dates for the given day of the week
            return all_dates[all_dates.day_name() == day_of_week.title()]

        # Generate full schedule
        full_schedule_data = []
        for index, row in opening_shifts.iterrows():
            dates = get_dates_for_day(row['DAY_OF_WEEK'], start_date, end_date)
            for date in dates:
                if row['BREAKFAST'] == 'Y':
                    full_schedule_data.append({'COMPANY_NAME': row['COMPANY_NAME'], 
                                               'KICHEN_NAME': row['KICHEN_NAME'], 
                                               'OPERATION_DATE': date, 
                                               'SHIFT_ID': 'BREAKFAST'})
                if row['BRUNCH'] == 'Y':
                    full_schedule_data.append({'COMPANY_NAME': row['COMPANY_NAME'], 
                                               'KICHEN_NAME': row['KICHEN_NAME'], 
                                               'OPERATION_DATE': date, 
                                               'SHIFT_ID': 'BRUNCH'})
                if row['LUNCH'] == 'Y':
                    full_schedule_data.append({'COMPANY_NAME': row['COMPANY_NAME'], 
                                               'KICHEN_NAME': row['KICHEN_NAME'], 
                                               'OPERATION_DATE': date, 
                                               'SHIFT_ID': 'LUNCH'})
                if row['AFTERNOON_TEA'] == 'Y':
                    full_schedule_data.append({'COMPANY_NAME': row['COMPANY_NAME'], 
                                               'KICHEN_NAME': row['KICHEN_NAME'], 
                                               'OPERATION_DATE': date, 
                                               'SHIFT_ID': 'AFTERNOON_TEA'})
                if row['DINNER'] == 'Y':
                    full_schedule_data.append({'COMPANY_NAME': row['COMPANY_NAME'], 
                                               'KICHEN_NAME': row['KICHEN_NAME'], 
                                               'OPERATION_DATE': date, 
                                               'SHIFT_ID': 'DINNER'})
        logger.info("Generated full schedule data")

        full_schedule_df = pd.DataFrame(full_schedule_data)
        full_schedule_pbl = full_schedule_df.merge(firstdate, on=['COMPANY_NAME', 'KICHEN_NAME'], how='left')
        full_schedule_pbl2 = full_schedule_pbl[full_schedule_pbl['OPERATION_DATE'] > full_schedule_pbl['FirstDate']]
        full_schedule_pbl2['YEAR'] = full_schedule_pbl2['OPERATION_DATE'].dt.year
        full_schedule_pbl2['MONTH'] = full_schedule_pbl2['OPERATION_DATE'].dt.month

        # Adding start and end date to consistency
        bounds = full_schedule_pbl2.groupby(['COMPANY_NAME', 'KICHEN_NAME']).agg(
            START_DATE=('OPERATION_DATE','min'),
            END_DATE=('OPERATION_DATE','max')
        ).reset_index()

        # Strip time
        bounds['START_DATE'] = bounds['START_DATE'].dt.date
        bounds['END_DATE'] = bounds['END_DATE'].dt.date

        total_shift_per_month = full_schedule_pbl2.groupby(['COMPANY_NAME', 'KICHEN_NAME', 'YEAR', 'MONTH']).agg(
            TOTAL_SHIFTS=('SHIFT_ID', 'count')
        ).reset_index()
        logger.info("Calculated total shifts per month")

        closed['YEAR'] = closed['CLOSE_DATE'].dt.year
        closed['MONTH'] = closed['CLOSE_DATE'].dt.month
        closed = closed.merge(firstdate, on=['COMPANY_NAME', 'KICHEN_NAME'], how='left')
        closed = closed[closed['CLOSE_DATE'] > closed['FirstDate']]
        closed = closed.drop(['FirstDate', 'COUNTRY_CODE'], axis=1)
        closed_shifts_count = closed.groupby(['COMPANY_NAME', 'KICHEN_NAME', 'YEAR', 'MONTH'])['CLOSE_DATE'].count().reset_index(name='CLOSED_SHIFTS')
        logger.info("Calculated closed shifts count")

        open_comp = pd.merge(left=total_shift_per_month, right=comp_set_per_month_old, on=['COMPANY_NAME', 'KICHEN_NAME', 'YEAR', 'MONTH'], how='left')
        final_comp_per_month = open_comp.merge(closed_shifts_count, on=['COMPANY_NAME', 'KICHEN_NAME', 'YEAR', 'MONTH'], how='left')
        dcon_per_month = final_comp_per_month.copy()

        dcon_per_month['COMP_SHIFTS_OLD'] = np.nan_to_num(dcon_per_month['COMP_SHIFTS_OLD'])
        dcon_per_month['CLOSED_SHIFTS'] = np.nan_to_num(dcon_per_month['CLOSED_SHIFTS'])
        dcon_per_month['CONSISTENCY_OLD'] = (dcon_per_month['COMP_SHIFTS_OLD'] / (dcon_per_month['TOTAL_SHIFTS'] - dcon_per_month['CLOSED_SHIFTS'])).round(2)
        logger.info("Calculated data consistency per month")

        dcon_overall = dcon_per_month.groupby(['COMPANY_NAME', 'KICHEN_NAME']).agg(
            TOTAL_SHIFTS=('TOTAL_SHIFTS', 'sum'),
            COMP_SHIFTS_OLD=('COMP_SHIFTS_OLD', 'sum'),
            CLOSED_SHIFTS=('CLOSED_SHIFTS', 'sum')
        )
        dcon_overall['CONSISTENCY_OLD'] = (dcon_overall['COMP_SHIFTS_OLD'] / (dcon_overall['TOTAL_SHIFTS'] - dcon_overall['CLOSED_SHIFTS'])).round(2)
        logger.info("Calculated overall data consistency")

        overall_column = {'CONSISTENCY_OLD': 'CONSISTENCY', 'COMP_SHIFTS_OLD': 'COMPLETE_SHIFTS'}
        month_column = {'CONSISTENCY_OLD': 'CONSISTENCY', 'COMP_SHIFTS_OLD': 'COMPLETE_SHIFTS'}
        month = dcon_per_month.rename(columns=month_column)
        overall = dcon_overall.rename(columns=overall_column)
        overall_2 = overall.merge(bounds, on=['COMPANY_NAME', 'KICHEN_NAME'], how='left')

        temp_dir = tempfile.gettempdir()
        file_path = os.path.join(temp_dir, f"{company_name}_Consistency.xlsx")
        with pd.ExcelWriter(file_path, engine='xlsxwriter') as writer:
            month.to_excel(writer, sheet_name='Monthly Consistency', index=False)
            overall_2.to_excel(writer, sheet_name='Overall Consistency', index=False)
        logger.info("Excel file created successfully")
        return send_file(file_path, as_attachment=True)

    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        return f"An error occurred: {str(e)}"

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=10000)
    pass

