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
    database = os.getenv('database')
    
    # Encode the password to handle special characters
    encoded_password = urllib.parse.quote_plus(password)
    
    # Create the connection string with the encoded password
    connection_str = f"mysql+mysqlconnector://{user}:{encoded_password}@{host}/{database}"
    engine = sqlalchemy.create_engine(connection_str)
    return engine

def group_by_parent_company(df, column_name='COMPANY_NAME'):
    # Define the mapping of keywords to parent companies
    parent_company_mapping = {
        'Hyatt': 'Hyatt',
        'Andaz': 'Hyatt',
        'Alila': 'Hyatt',
        'Fuji Speedway': 'Hyatt',
        'Constance': 'Constance',
        'Marriott': 'Marriott',
        'Courtyard': 'Marriott',
        'Sheraton': 'Marriott',
        'Chapter': 'Marriott',
        'Aloft': 'Marriott',
        'Magic': 'Magic',
        'MCB': 'MCB',
        'RH': 'RESTHOTELS',
        'Hotel Lava': 'RESTHOTELS',
        'UBC': 'UBC',
        'Louvre': 'Jin Jiang',
        'J\'AIME': 'J\'AIME'
    }

    # Function to get parent company based on the mapping
    def get_parent_company(company_name):
        for keyword, parent in parent_company_mapping.items():
            if keyword in company_name:
                return parent
        return company_name  # Return the original name if no match is found

    # Apply the function to the specified column
    df['PARENT_COMPANY'] = df[column_name].apply(get_parent_company)
    return df

# Create engine object
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

@app.route('/process_dcon', methods=['GET','POST'])
def process_dcon():
    logger.info("Processing the dcon")
    company_name = request.form['company_name']
    end_date = request.form['end_date']
    start_date = '2021-03-12'
    try:
        if end_date <= '2024-06-30':
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
            full_schedule_pbl2 = full_schedule_pbl.copy()
            full_schedule_pbl2 = full_schedule_pbl[full_schedule_pbl['OPERATION_DATE'] > full_schedule_pbl['FirstDate']]
            full_schedule_pbl2 = full_schedule_pbl2.copy()
            full_schedule_pbl2['YEAR'] = full_schedule_pbl2['OPERATION_DATE'].dt.year
            full_schedule_pbl2['MONTH'] = full_schedule_pbl2['OPERATION_DATE'].dt.month

            # Adding start and end date to consistency
            bounds = full_schedule_pbl2.groupby(['COMPANY_NAME', 'KICHEN_NAME']).agg(
                START_DATE=('OPERATION_DATE','min'),
                END_DATE=('OPERATION_DATE','max')
            ).reset_index()

            # Strip time
            bounds['START_DATE'] = bounds['START_DATE'].dt.strftime('%d-%b-%Y')
            bounds['END_DATE'] = bounds['END_DATE'].dt.strftime('%d-%b-%Y')

            total_shift_per_month = full_schedule_pbl2.groupby(['COMPANY_NAME', 'KICHEN_NAME', 'YEAR', 'MONTH']).agg(
                TOTAL_SHIFTS=('SHIFT_ID', 'count')
            ).reset_index()
            logger.info("Calculated total shifts per month")
            def func(x):
                try:
                    if opening_shifts.where((opening_shifts['DAY_OF_WEEK']==x['CLOSE_DATE'].day_name().upper()) &
                                            (opening_shifts['KICHEN_NAME']==x['KICHEN_NAME'])&(opening_shifts['COMPANY_NAME']==x['COMPANY_NAME']))[x['SHIFT_ID']].dropna().item()=='N':
                        return False
                except:
                    return True
                return True
            closed['IS_REDONDANT']=closed.apply(lambda x: 'N' if func(x) else 'Y', axis=1)

            #We drop the duplicates
            closed = closed[closed['IS_REDONDANT']=='N']

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

            overall_column = {'CONSISTENCY_OLD': 'CONSISTENCY', 'COMP_SHIFTS_OLD': 'COMP_SHIFTS'}
            month_column = {'CONSISTENCY_OLD': 'CONSISTENCY', 'COMP_SHIFTS_OLD': 'COMP_SHIFTS'}
            month = dcon_per_month.rename(columns=month_column)
            overall = dcon_overall.rename(columns=overall_column)
            overall_2 = overall.merge(bounds, on=['COMPANY_NAME', 'KICHEN_NAME'], how='left')
            
            month['OPERATION_DATE'] = pd.to_datetime(month[['YEAR', 'MONTH']].assign(DAY=1))

            # Step 2: Format the OPERATION_DATE to YYYY-MMM
            month['OPERATION_DATE'] = month['OPERATION_DATE'].dt.strftime('%Y-%b')

            # Drop the original YEAR and MONTH columns if needed
            month = month.drop(columns=['YEAR', 'MONTH'])
            month = month[['COMPANY_NAME','KICHEN_NAME','OPERATION_DATE','CONSISTENCY','TOTAL_SHIFTS','COMP_SHIFTS','CLOSED_SHIFTS']]
        else:
            # Fetch data
            fwcv = fetch_fwcv(engine, company_name, end_date='2024-06-30')
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

            closed = fetch_closed_shifts(engine, company_name, end_date='2024-06-30')
            
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
            def get_dates_for_day(day_of_week, start_date, end_date='2024-06-30'):
            # Create a date range for the period of interest
                all_dates = pd.date_range(start=start_date, end=end_date)
                # Filter dates for the given day of the week
                return all_dates[all_dates.day_name() == day_of_week.title()]

            # Generate full schedule
            full_schedule_data = []
            for index, row in opening_shifts.iterrows():
                dates = get_dates_for_day(row['DAY_OF_WEEK'], start_date, end_date='2024-06-30')
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
            full_schedule_pbl2 = full_schedule_pbl.copy()
            full_schedule_pbl2 = full_schedule_pbl[full_schedule_pbl['OPERATION_DATE'] > full_schedule_pbl['FirstDate']]
            full_schedule_pbl2 = full_schedule_pbl2.copy()
            full_schedule_pbl2['YEAR'] = full_schedule_pbl2['OPERATION_DATE'].dt.year
            full_schedule_pbl2['MONTH'] = full_schedule_pbl2['OPERATION_DATE'].dt.month

            # Adding start and end date to consistency
            bounds = full_schedule_pbl2.groupby(['COMPANY_NAME', 'KICHEN_NAME']).agg(
                START_DATE=('OPERATION_DATE','min'),
                END_DATE=('OPERATION_DATE','max')
            ).reset_index()

            # Strip time
            bounds['START_DATE'] = bounds['START_DATE'].dt.strftime('%Y-%m-%d')
            bounds['END_DATE'] = bounds['END_DATE'].dt.strftime('%Y-%m-%d')

            total_shift_per_month = full_schedule_pbl2.groupby(['COMPANY_NAME', 'KICHEN_NAME', 'YEAR', 'MONTH']).agg(
                TOTAL_SHIFTS=('SHIFT_ID', 'count')
            ).reset_index()
            logger.info("Calculated total shifts per month")
            def func(x):
                try:
                    if opening_shifts.where((opening_shifts['DAY_OF_WEEK']==x['CLOSE_DATE'].day_name().upper()) &
                                            (opening_shifts['KICHEN_NAME']==x['KICHEN_NAME'])&(opening_shifts['COMPANY_NAME']==x['COMPANY_NAME']))[x['SHIFT_ID']].dropna().item()=='N':
                        return False
                except:
                    return True
                return True
            closed['IS_REDONDANT']=closed.apply(lambda x: 'N' if func(x) else 'Y', axis=1)

            #We drop the duplicates
            closed = closed[closed['IS_REDONDANT']=='N']

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

            overall_column = {'CONSISTENCY_OLD': 'CONSISTENCY', 'COMP_SHIFTS_OLD': 'COMP_SHIFTS'}
            month_column = {'CONSISTENCY_OLD': 'CONSISTENCY', 'COMP_SHIFTS_OLD': 'COMP_SHIFTS'}
            month = dcon_per_month.rename(columns=month_column)
            overall = dcon_overall.rename(columns=overall_column)
            overall_dcon_jun = overall.merge(bounds, on=['COMPANY_NAME', 'KICHEN_NAME'], how='left')

            
            month['OPERATION_DATE'] = pd.to_datetime(month[['YEAR', 'MONTH']].assign(DAY=1))



            # Drop the original YEAR and MONTH columns if needed
            month = month.drop(columns=['YEAR', 'MONTH'])
            month_dcon_jun = month[['COMPANY_NAME','KICHEN_NAME','OPERATION_DATE','CONSISTENCY','TOTAL_SHIFTS','COMP_SHIFTS','CLOSED_SHIFTS']]

            overall_dcon_after=DCON(engine=engine,company_name=company_name, start_date='2024-07-01', end_date=end_date, grouping='overall')
            monthly_dcon_after=DCON(engine=engine,company_name=company_name, start_date='2024-07-01' ,end_date=end_date, grouping='monthly')
            logger.info("Calculated overall and monthly dcon after jun 2024")
           # change date format for after jun
            # concat both monthly dfs
            month = pd.concat([month_dcon_jun, monthly_dcon_after], axis=0)
            month = month.sort_values(by=['COMPANY_NAME', 'KICHEN_NAME', 'OPERATION_DATE'])
            month['OPERATION_DATE'] = month['OPERATION_DATE'].dt.strftime('%b-%Y')
            
            overall_dcon_after['END_DATE'] = str(end_date)
            overall_dcon_after['START_DATE'] = '2024-07-01'
            # concat both overall dfs, turn into datetime and just aggregate for bounds and consistency
            overall_2 = pd.concat([overall_dcon_jun, overall_dcon_after], axis=0)
            overall_2['START_DATE'] = pd.to_datetime(overall_2['START_DATE'])
            overall_2['END_DATE'] = pd.to_datetime(overall_2['END_DATE'])
            overall_2 = overall_2.groupby(['COMPANY_NAME', 'KICHEN_NAME'], as_index=False).agg(
                {'TOTAL_SHIFTS':'sum',
                 'CLOSED_SHIFTS':'sum',
                 'COMP_SHIFTS':'sum',
                 'START_DATE':'min',
                 'END_DATE':'max',
                 })
            # calcuclate dcon and reorg columns
            overall_2['CONSISTENCY'] = (overall_2['COMP_SHIFTS'] / (overall_2['TOTAL_SHIFTS'] - overall_2['CLOSED_SHIFTS'])).round(2)
            overall_2 = overall_2[['COMPANY_NAME','KICHEN_NAME','CONSISTENCY','TOTAL_SHIFTS','CLOSED_SHIFTS','COMP_SHIFTS','START_DATE','END_DATE']]

        # Generate charts from the `month` DataFrame
        charts = plot_graph(month)
        logger.info("Plotted graphs")

        # Store the Excel file for download
        temp_dir = tempfile.gettempdir()
        file_path = os.path.join(temp_dir, f"{company_name}_dcon_data.xlsx")
        
        with pd.ExcelWriter(file_path, engine='xlsxwriter') as writer:
            month.to_excel(writer, sheet_name='Monthly Data', index=False)
            overall_2.to_excel(writer, sheet_name='Overall Data', index=True)

        # Convert dataframes to HTML for the tables
        month_table = month.to_html(classes='table table-striped table-bordered table-hover', index=False)
        overall_table = overall_2.to_html(classes='table table-striped table-bordered table-hover', index=True)
        
        # Render the HTML template with charts and data tables
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

@app.route('/form_wdcon', methods=['GET', 'POST'])
def form_wdcon():
    return render_template('form_wdcon.html')

@app.route('/process_wdcon', methods=['POST'])
def process_wdcon():
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
            return render_template(
                'weekly_dcon.html',
                week_table=constance.to_html(classes='table table-striped table-bordered table-hover', index=False),
                avg_per_parent_table=avg_per_parent_table,
                download_link=download_link, parent_company=parent.capitalize()
            )
        elif parent == 'hyatt':
            return render_template(
                'weekly_dcon.html',
                week_table=hyatt.to_html(classes='table table-striped table-bordered table-hover', index=False),
                avg_per_parent_table=avg_per_parent_table,
                download_link=download_link, parent_company=parent.capitalize()
            )
        else:
            return render_template(
                'weekly_dcon.html',
                week_table=marriott.to_html(classes='table table-striped table-bordered table-hover', index=False),
                avg_per_parent_table=avg_per_parent_table,
                download_link=download_link, parent_company='Marriott & Others')
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        return f"An error occurred: {str(e)}"
        
@app.route('/download_excel/<filename>')
def download_excel(filename):
    temp_dir = tempfile.gettempdir()
    file_path = os.path.join(temp_dir, filename)
    return send_file(file_path, as_attachment=True)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8008, debug=True)




        

