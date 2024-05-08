from flask import Flask, render_template, request, send_file
import pandas as pd
import sqlalchemy
import configparser
import os

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

# Fetch data based on form input
def fetch_data(engine, company_name, start_date, end_date):
    query = f"""
    SELECT 
        DATE(kfw.OPERATION_DATE) as OPERATION_DATE,
        cp.COMPANY_NAME,
            ks.KICHEN_NAME,
    kfw.SHIFT_ID,
    kfw.IGD_CATEGORY_ID,
    kfw.IGD_FOODTYPE_ID,
    kfw.AMOUNT 
    FROM 
        KITCHEN_FOOD_WASTE kfw 
    JOIN 
        KITCHEN_STATION ks ON kfw.KC_STT_ID = ks.KC_STT_ID 
    JOIN 
        COMPANY_PROFILE cp ON ks.CPN_PF_ID = cp.CPN_PF_ID
    WHERE kfw.ACTIVE ='Y' and 
    cp.COMPANY_STATUS = 'ACTIVE' and 
    ks.KICHEN_STATUS = 'Y' and
    ks.ACTIVE = 'Y' and cp.COMPANY_NAME LIKE '%{company_name}%' and (kfw.OPERATION_DATE BETWEEN '{start_date}' AND '{end_date}')
    ORDER BY kfw.OPERATION_DATE;"""
    return pd.read_sql_query(query, engine)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/form_extractor')
def form_extractor():
    return render_template('form_extractor.html')

@app.route('/process_extractor', methods=['POST'])
def process_extractor():
    company_name = request.form['company_name']
    start_date = request.form['start_date']
    end_date = request.form['end_date']

    # Load configuration and create a database connection
    config = load_configuration()
    engine = create_connection(config)

    try:
        # Fetch data
        fw = fetch_data(engine, company_name, start_date, end_date)

        if fw.empty:
            return "No data found for the given parameters."

        # Process data into a pivot table
        np_fw = fw[fw['IGD_CATEGORY_ID'] != 'PLATE']
        pivot = np_fw.pivot_table(index=['COMPANY_NAME', 'KICHEN_NAME'], columns='IGD_FOODTYPE_ID', values='AMOUNT', aggfunc='sum').reset_index()
        pivot['START'] = np_fw['OPERATION_DATE'].min()
        pivot['END'] = np_fw['OPERATION_DATE'].max()

        # Save to CSV
        home_dir = os.path.expanduser('~')
        filename = f'{company_name}_total_fw_kg.csv'
        file_path = os.path.join(home_dir, 'Documents', filename)
        pivot.to_csv(file_path, index=False)

        # Return the file as a download
        return send_file(file_path, as_attachment=True)

    except Exception as e:
        return f"An error occurred: {str(e)}"

if __name__ == '__main__':
    app.run(debug=True)

