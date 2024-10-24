import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
import pandas as pd
from datetime import timedelta, datetime
import urllib.parse
import sqlalchemy
import numpy as np
import openpyxl
import sys
import codecs
import logging
import io

# Set up logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

load_dotenv()

# Database connection parameters
USERNAME = os.getenv('user')
HOST = os.getenv('host')
DATABASE_NAME = os.getenv('database')
PASSWORD = urllib.parse.quote_plus(os.getenv('password'))


def escape_sql_string(value):
    if not isinstance(value, str):
        return value
    # List of characters that need to be escaped
    characters_to_escape = ["\\", "'", "\"", "%", "_"]
    for char in characters_to_escape:
        value = value.replace(char, "\\" + char)
    return value

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

# Old kitchens that are not included in the calculations

trial_kitchens = [
    ("Crescendo", "Anantara The Palm Dubai"),
    ("JW Vancouver", "JW Marriott PARQ Vancouver"),
    ("Hennur", "Geist Brewing Co.")
]

demo_kitchens = [
    ("All day dining", "Demo Urban Hotel"),
    ("SR (Add BL, April)", "Aloft dummy"),
    ("Silk Road", "Aloft dummy"),
    ("Deer Hunter", "Constance Belle Mare Plage"),
    ("Staff Canteen", "Constance Belle Mare Plage"),
    ("La Spiaggia", "Constance Belle Mare Plage"),
    ("Indigo", "Constance Belle Mare Plage"),
    ("La Kaze", "Constance Belle Mare Plage"),
    ("Blue Penny", "Constance Belle Mare Plage"),
    ("La Citronelle", "Constance Belle Mare Plage"),
    ("Le Swing", "Constance Belle Mare Plage"),
    ("Cafe", "FIT Demo kitchen"),
    ("All Day Dining", "FIT Demo kitchen"),
    ("Canteen", "FIT Demo kitchen"),
    ("CAFE", "FIT Kitchen"),
    ("CANTEEN", "FIT Kitchen"),
    ("M ON22", "LIGHTBLUE"),
    ("Jaafaiy", "LIGHTBLUE"),
    ("Mamadoo", "Mamadoo Company Limited"),
    
    # Newly added demo kitchens
    ("Asian", "Mock Canteen"),
    ("Main kitchen", "Mock Canteen"),
    ("Noodle", "Mock Canteen"),
    ("Salad", "Mock Canteen"),
    ("Vegetarian", "Mock Canteen"),
    ("Western", "Mock Canteen"),
    ("Null kitchen", "Name of Hotel")
]


excluded_kitchens_set = set(trial_kitchens + demo_kitchens)


# Create a connection to the MySQL database
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

# Kitchen names (New function, if dummies is False, then it will get the old restaurants as well and aggregate the full date range, 2024-07-10)
def get_kitchen(company_name=None, Dummies=True, Expired=False, Emails=False):
    # Establish connection to the database
    engine = create_connection()

    # Base query conditions
    query_conditions = """
        ks.ACTIVE = 'Y' AND 
        cp.ACTIVE = 'Y' AND 
        cp.COMPANY_STATUS = 'ACTIVE' AND 
        ks.KICHEN_STATUS = 'Y'
    """

    if company_name:
        query_conditions += f" AND cp.COMPANY_NAME = '{company_name}'"

    # Handle Dummies (trial and demo kitchens)
    if Dummies:
        conditions = []
        for restaurant, company in trial_kitchens + demo_kitchens:
            if company:
                conditions.append(f"(ks.KICHEN_NAME = '{restaurant}' AND cp.COMPANY_NAME = '{company}')")
            else:
                conditions.append(f"ks.KICHEN_NAME = '{restaurant}'")
        if conditions:
            conditions_str = " AND NOT (" + " OR ".join(conditions) + ")"
            query_conditions += conditions_str

    # Handle expired licenses if Expired is False
    if not Expired:
        current_date = datetime.now().strftime('%Y-%m-%d')
        query_conditions += f" AND ca.LICENSE_EXPIRE_DATE > '{current_date}'"

    # Query to retrieve kitchen, company, emails (with fallback to COMPANY_REGISTER)
    query = f"""
        SELECT
            ks.KICHEN_NAME as kitchen_name,
            cp.COMPANY_NAME as company_name,
            COALESCE(cp.WEEKLY_REPORT_MAIL_TO, cr.WEEKLY_REPORT_EMAIL) as Mail_To,
            cp.WEEKLY_REPORT_MAIL_CC as Mail_Cc,
            MIN(ca.LICENSE_START_DATE) as LICENSE_START_DATE,
            MAX(ca.LICENSE_EXPIRE_DATE) as LICENSE_EXPIRE_DATE
        FROM
            KITCHEN_STATION ks
        JOIN
            COMPANY_PROFILE cp ON ks.CPN_PF_ID = cp.CPN_PF_ID
        LEFT JOIN
            COMPANY_REGISTER cr ON cp.CPN_PF_ID = cr.CPN_PF_ID
        LEFT JOIN
            COMPANY_ACTIVATE ca ON ks.CPN_PF_ID = ca.CPN_PF_ID
        WHERE
            {query_conditions}
        GROUP BY
            ks.KICHEN_NAME, cp.COMPANY_NAME, cp.WEEKLY_REPORT_MAIL_TO, cp.WEEKLY_REPORT_MAIL_CC, cr.WEEKLY_REPORT_EMAIL
    """

    # Print query for debugging purposes
    print("Final SQL Query:\n", query)

    # Execute the query and load the data into a DataFrame
    df = pd.read_sql(query, engine)

    # Sort and remove duplicates conditionally based on Emails flag
    if Emails:
        df = df.drop_duplicates(subset=['kitchen_name', 'company_name', 'LICENSE_START_DATE', 'LICENSE_EXPIRE_DATE', "Mail_To", "Mail_Cc"])
    else:
        df = df.drop_duplicates(subset=['kitchen_name', 'company_name', 'LICENSE_START_DATE', 'LICENSE_EXPIRE_DATE'])

    # Print the DataFrame to ensure proper handling of dummies, expired, and emails
    print("Retrieved Kitchen Data with Dummies and Expired Handling:")
    print(df)

    # Close the database connection
    engine.dispose()

    return df

# Full DF of company names
def get_companies(Dummies=True, Expired=False):
    # Establish connection to the database
    engine = create_connection()

    # Construct the SQL query conditions
    query_conditions = """
        ks.ACTIVE = 'Y' AND 
        cp.ACTIVE = 'Y' AND 
        cp.COMPANY_STATUS = 'ACTIVE' AND 
        ks.KICHEN_STATUS = 'Y'
    """

    # Define the base SQL query to aggregate licenses
    license_aggregation_query = """
        SELECT
            ks.KICHEN_NAME,
            cp.COMPANY_NAME,
            MIN(ca.LICENSE_START_DATE) as LICENSE_START_DATE,
            MAX(ca.LICENSE_EXPIRE_DATE) as LICENSE_EXPIRE_DATE
        FROM
            KITCHEN_STATION ks
        JOIN
            COMPANY_PROFILE cp ON ks.CPN_PF_ID = cp.CPN_PF_ID
        LEFT JOIN
            COMPANY_ACTIVATE ca ON ks.CPN_PF_ID = ca.CPN_PF_ID
        WHERE
            ks.ACTIVE = 'Y' AND
            cp.ACTIVE = 'Y' AND
            cp.COMPANY_STATUS = 'ACTIVE' AND
            ks.KICHEN_STATUS = 'Y'
        GROUP BY
            ks.KICHEN_NAME, cp.COMPANY_NAME
    """

    licenses_df = pd.read_sql_query(license_aggregation_query, engine)

    if Dummies or Expired:
        conditions = []
        if Dummies:
            for restaurant, company in trial_kitchens + demo_kitchens:
                if company:
                    conditions.append(
                        f"(ks.KICHEN_NAME = '{restaurant}' AND cp.COMPANY_NAME = '{company}')")
                else:
                    conditions.append(f"ks.KICHEN_NAME = '{restaurant}'")

            if Expired:
                conditions = [
                    cond for cond in conditions if "trial" in cond or "demo" in cond]

        if conditions:
            conditions_str = " AND NOT (" + " OR ".join(conditions) + ")"
            query_conditions += conditions_str

        if Dummies and not Expired:
            current_date = datetime.now()
            licenses_df = licenses_df[licenses_df['LICENSE_EXPIRE_DATE']
                                      >= current_date]

    # Define the base SQL query
    query = f"""
    SELECT
        cp.COMPANY_NAME as company_name,
        MIN(ca.LICENSE_START_DATE) as LICENSE_START_DATE,
        MAX(ca.LICENSE_EXPIRE_DATE) as LICENSE_EXPIRE_DATE
    FROM
        COMPANY_PROFILE cp
    JOIN
        KITCHEN_STATION ks ON ks.CPN_PF_ID = cp.CPN_PF_ID
    LEFT JOIN
        COMPANY_ACTIVATE ca ON ks.CPN_PF_ID = ca.CPN_PF_ID
    WHERE
        {query_conditions}
    """

    # Group by company name to aggregate dates
    query += " GROUP BY cp.COMPANY_NAME"
    
    # Add parent grouping
    df = pd.read_sql(query, engine)
    df = group_by_parent_company(df, column_name='company_name')
    

    # Execute the query and load the data into a DataFrame
    df = pd.read_sql(query, engine)
    df = df.sort_values(by='company_name')

    # Close the database connection
    engine.dispose()

    return df

# Get all the inputs
def get_all_input(start_date, end_date, kitchen_name=None):
    # Establish connection to the database
    engine = create_connection()

    # Define the base SQL query
    query = f"""
    SELECT
        cp.COMPANY_NAME as company_name,
        ks.KICHEN_NAME as kitchen_name,
        kfw.OPERATION_DATE as date,
        kfw.AMOUNT as input,
        kfw.SHIFT_ID as shift,
        kfw.UPDATE_DATE as update_date,
        kfw.IGD_CATEGORY_ID as category,
        kfw.IGD_FOODTYPE_ID as food_type,
        cp.WEIGHT_UNIT_CODE as weight_unit
    FROM
        KITCHEN_STATION ks
    JOIN
        KITCHEN_FOOD_WASTE kfw ON ks.KC_STT_ID = kfw.KC_STT_ID
    JOIN
        COMPANY_PROFILE cp ON ks.CPN_PF_ID = cp.CPN_PF_ID
    WHERE
        kfw.OPERATION_DATE BETWEEN '{start_date}' AND '{end_date}' AND
        kfw.ACTIVE = 'Y'
    """

    # If kitchen_name is provided, add the condition to the query
    if kitchen_name:
        query += f" AND ks.KICHEN_NAME = '{kitchen_name}'"

    # Order by date and shift
    query += " ORDER BY kfw.OPERATION_DATE, kfw.SHIFT_ID"

    # Execute the query and load the data into a DataFrame
    df = pd.read_sql(query, engine)

    # Close the database connection
    engine.dispose()

    # Filter out excluded kitchens
    df = df[~df[['kitchen_name', 'company_name']].apply(
        tuple, axis=1).isin(excluded_kitchens_set)]

    # Filter out inputs that are 0g
    df = df[df['input'] > 0]

    conversion_factors = {
        'KILOGRAM': 1000,   # kilograms to grams
        'POUND': 453.592,  # pounds to grams
        'GRAM': 1         # grams to grams
    }

    # Convert input amounts to grams
    df['input_in_grams'] = df.apply(
        lambda row: row['input'] * conversion_factors.get(row['weight_unit'], 1), axis=1)

    return df

# Helper function that is used to calculate g_cover
def get_food_waste_and_covers(start_date='2000-01-01', end_date=datetime.now(), company_name=None, restaurant_name=None, shift=None, category=None, food_type=None, CONS=False, SHIFT_ID=True, IGD_FOODTYPE_ID=True, IGD_CATEGORY_ID=True, OPERATION_DATE=True, Dummies=True, Expired=False):
    engine = create_connection()

    # Fetch kitchens if company_name and restaurant_name are None
    if not company_name and not restaurant_name:
        kitchen_df = get_kitchen(Dummies=Dummies, Expired=Expired)
        company_name = kitchen_df['company_name'].unique().tolist()
        restaurant_name = kitchen_df['kitchen_name'].unique().tolist()

    # Initialize a list to hold WHERE conditions
    where_conditions = ["kfw.ACTIVE = 'Y'"]
    if CONS:
        where_conditions.append("kfw.COMPLETE='Y'")
    
    # Build the condition for company_name
    if company_name:
        if isinstance(company_name, list) and company_name:  # Check if it's a list and not empty
            company_names_str = "', '".join([escape_sql_string(c) for c in company_name])
            company_names_str = f"('{company_names_str}')"
            where_conditions.append(f"cp.COMPANY_NAME IN {company_names_str}")
        else:
            where_conditions.append(f"cp.COMPANY_NAME = '{escape_sql_string(company_name)}'")
    
    # Build the condition for restaurant_name
    if restaurant_name:
        if isinstance(restaurant_name, list) and restaurant_name:  # Check if it's a list and not empty
            kitchen_names_str = "', '".join([escape_sql_string(r) for r in restaurant_name])
            kitchen_names_str = f"('{kitchen_names_str}')"
            where_conditions.append(f"ks.KICHEN_NAME IN {kitchen_names_str}")
        else:
            where_conditions.append(f"ks.KICHEN_NAME = '{escape_sql_string(restaurant_name)}'")

    if shift and shift != 'ALL':
        where_conditions.append(f'kfw.SHIFT_ID="{shift.upper()}"')
    if category and category != 'ALL':
        where_conditions.append(f'kfw.IGD_CATEGORY_ID="{category.upper()}"')
    if food_type and food_type != 'ALL':
        where_conditions.append(f'kfw.IGD_FOODTYPE_ID="{food_type.upper()}"')

    # Add date range condition
    where_conditions.append(f"kfw.OPERATION_DATE BETWEEN '{start_date}' AND '{end_date}'")

    # Construct the WHERE clause by joining conditions with ' AND '
    where_clause = ' AND\n    '.join(where_conditions)

    # Adjust CV_AMOUNT based on flags
    if IGD_FOODTYPE_ID or IGD_CATEGORY_ID:
        CV_AMOUNT = "kc.AMOUNT"
    elif not SHIFT_ID or not OPERATION_DATE:
        CV_AMOUNT = "SUM(kc.AMOUNT)"
    else:
        CV_AMOUNT = "kc.AMOUNT"

    # Final SQL query construction
    fw_cv_q = f"""
    SELECT
        DATE(kfw.OPERATION_DATE) as OPERATION_DATE,
        cp.COMPANY_NAME,
        ks.KICHEN_NAME,
        kfw.SHIFT_ID,
        kfw.IGD_CATEGORY_ID,
        kfw.IGD_FOODTYPE_ID,
        SUM(kfw.AMOUNT) as FW,
        {CV_AMOUNT} as CV,
        cp.WEIGHT_UNIT_CODE as weight_unit
    FROM
        lightblue.KITCHEN_FOOD_WASTE kfw
    JOIN
        lightblue.KITCHEN_STATION ks ON kfw.KC_STT_ID = ks.KC_STT_ID
    JOIN
        lightblue.COMPANY_PROFILE cp ON ks.CPN_PF_ID = cp.CPN_PF_ID
    LEFT JOIN
        lightblue.KITCHEN_COVER kc ON kc.KC_STT_ID = ks.KC_STT_ID AND kc.SHIFT_ID = kfw.SHIFT_ID AND kc.OPERATION_DATE = kfw.OPERATION_DATE
    LEFT JOIN
        lightblue.COMPANY_ACTIVATE ca ON ks.CPN_PF_ID = ca.CPN_PF_ID
    WHERE
        {where_clause}
    GROUP BY
        cp.COMPANY_NAME, ks.KICHEN_NAME, kfw.OPERATION_DATE, kfw.SHIFT_ID, kfw.IGD_CATEGORY_ID, kfw.IGD_FOODTYPE_ID
    """

    # Add grouping conditions based on flags
    if OPERATION_DATE:
        fw_cv_q += ", kfw.OPERATION_DATE"
    if SHIFT_ID:
        fw_cv_q += ", kfw.SHIFT_ID"
    if IGD_CATEGORY_ID:
        fw_cv_q += ", kfw.IGD_CATEGORY_ID"
    if IGD_FOODTYPE_ID:
        fw_cv_q += ", kfw.IGD_FOODTYPE_ID"

    # Conversion factors for different weight units
    conversion_factors = {
        'KILOGRAM': 1000,   # kilograms to grams
        'POUND': 453.592,  # pounds to grams
        'GRAM': 1         # grams to grams
    }

    # Query the data
    fwcv_comp = pd.read_sql_query(fw_cv_q, engine)
    fwcv_comp['OPERATION_DATE'] = pd.to_datetime(fwcv_comp['OPERATION_DATE'])
    
    # Convert FW (food waste) to grams based on the weight_unit
    fwcv_comp['FW_in_grams'] = fwcv_comp.apply(
        lambda row: row['FW'] * conversion_factors.get(row['weight_unit'], 1), axis=1
    )
    
    return fwcv_comp

# Get full covers for all restaurants for each shift
def get_covers(start_date='2000-01-01', end_date=datetime.now(), company_name=None, restaurant_name=None, shift='ALL', category='ALL', food_type='ALL', CONS=False, SHIFT_ID=True, OPERATION_DATE=True, Dummies=True, Expired=False):
    engine = create_connection()

    A = ""
    if CONS:
        A += "kfw.COMPLETE='Y' AND "
    if company_name:
        company_names_str = company_name
        A += f"cp.COMPANY_NAME IN ('{company_names_str}') AND "
    if restaurant_name:
        restaurant_names_str = restaurant_name
        A += f"ks.KICHEN_NAME IN ('{restaurant_names_str}') AND "
    if shift and shift != 'ALL':
        A += f'kfw.SHIFT_ID="{shift.upper()}" AND '
    if category and category != 'ALL':
        A += f'kfw.IGD_CATEGORY_ID="{category.upper()}" AND '
    if food_type and food_type != 'ALL':
        A += f'kfw.IGD_FOODTYPE_ID="{food_type.upper()}" AND '

    covers_query = f"""
    SELECT
        DATE(kfw.OPERATION_DATE) as OPERATION_DATE,
        cp.COMPANY_NAME,
        ks.KICHEN_NAME,
        kc.SHIFT_ID,
        SUM(kc.AMOUNT) as CV
    FROM
        lightblue.KITCHEN_FOOD_WASTE kfw
    JOIN
        lightblue.KITCHEN_STATION ks ON kfw.KC_STT_ID = ks.KC_STT_ID
    JOIN
        lightblue.COMPANY_PROFILE cp ON ks.CPN_PF_ID = cp.CPN_PF_ID
    LEFT JOIN
        lightblue.COMPANY_ACTIVATE ca ON ks.CPN_PF_ID = ca.CPN_PF_ID
    RIGHT JOIN
        lightblue.KITCHEN_COVER kc ON kc.KC_STT_ID = ks.KC_STT_ID AND kc.SHIFT_ID = kfw.SHIFT_ID AND kc.OPERATION_DATE = kfw.OPERATION_DATE
    WHERE
        kfw.ACTIVE = 'Y' AND
        {A}
        kfw.OPERATION_DATE BETWEEN '{start_date}' AND '{end_date}'
    """

    if Dummies or Expired:
        conditions = []
        if Dummies:
            for restaurant, company in trial_kitchens + demo_kitchens:
                if company:
                    conditions.append(
                        f"(ks.KICHEN_NAME = '{restaurant}' AND cp.COMPANY_NAME = '{company}')")
                else:
                    conditions.append(f"ks.KICHEN_NAME = '{restaurant}'")

            if Expired:
                conditions = [
                    cond for cond in conditions if "trial" in cond or "demo" in cond]

        if conditions:
            conditions_str = " AND NOT (" + " OR ".join(conditions) + ")"
            covers_query += conditions_str

        if Dummies and not Expired:
            covers_query += " AND ca.LICENSE_EXPIRE_DATE >= CURDATE()"

    covers_query += """
    GROUP BY 
        cp.COMPANY_NAME, ks.KICHEN_NAME
    """

    if OPERATION_DATE:
        covers_query += ", kfw.OPERATION_DATE"
    if SHIFT_ID:
        covers_query += ", kc.SHIFT_ID"

    covers_df = pd.read_sql_query(covers_query, engine)
    return covers_df

# Baseline Date (REPAIRED THE QUERY TO TAKE INTO ACCOUNT COMPANY AND RESTAURANT. IT ALWAYS RETURNS DF, 2024-07-03)
def baseline_date(company_name=None, restaurant_name=None, Dummies=True, Expired=False, baseline_selection=None):
    # Establish connection to the database
    engine = create_connection()
    query_conditions = ""

    # Aggregate licenses and filter based on the expiration date
    license_aggregation_query = """
        SELECT
            ks.KICHEN_NAME,
            cp.COMPANY_NAME,
            MIN(ca.LICENSE_START_DATE) as LICENSE_START_DATE,
            MAX(ca.LICENSE_EXPIRE_DATE) as LICENSE_EXPIRE_DATE
        FROM
            KITCHEN_STATION ks
        JOIN
            COMPANY_PROFILE cp ON ks.CPN_PF_ID = cp.CPN_PF_ID
        LEFT JOIN
            COMPANY_ACTIVATE ca ON ks.CPN_PF_ID = ca.CPN_PF_ID
        WHERE
            ks.ACTIVE = 'Y' AND
            cp.ACTIVE = 'Y' AND
            cp.COMPANY_STATUS = 'ACTIVE' AND
            ks.KICHEN_STATUS = 'Y'
            
        GROUP BY
            ks.KICHEN_NAME, cp.COMPANY_NAME
    """

    licenses_df = pd.read_sql_query(license_aggregation_query, engine)

    if Dummies or Expired:
        condition_list = []
        if Dummies:
            for restaurant, company in trial_kitchens + demo_kitchens:
                if company:
                    condition_list.append(
                        f"(ks.KICHEN_NAME = '{restaurant}' AND cp.COMPANY_NAME = '{company}')")
                else:
                    condition_list.append(f"ks.KICHEN_NAME = '{restaurant}'")

            if Expired:
                condition_list = [
                    cond for cond in condition_list if "trial" in cond or "demo" in cond]

        if condition_list:
            condition_str = " AND NOT (" + " OR ".join(condition_list) + ")"
            query_conditions += condition_str

        if Dummies and not Expired:
            current_date = datetime.now()
            licenses_df = licenses_df[licenses_df['LICENSE_EXPIRE_DATE']
                                      >= current_date]

    # Define the base SQL query
    query = f"""
    SELECT
        ks.KICHEN_NAME as restaurant_name,
        cp.COMPANY_NAME as company_name,
        kb.BASELINE_START_DATE as start_date,
        kb.BASELINE_END_DATE as end_date
    FROM
        KITCHEN_BASELINE kb
    JOIN
        KITCHEN_STATION ks ON kb.KC_STT_ID = ks.KC_STT_ID
    JOIN
        COMPANY_PROFILE cp ON ks.CPN_PF_ID = cp.CPN_PF_ID
    LEFT JOIN
        COMPANY_ACTIVATE ca ON ks.CPN_PF_ID = ca.CPN_PF_ID
    WHERE
        kb.ACTIVE = 'Y' AND
        cp.ACTIVE = 'Y' AND
        cp.COMPANY_STATUS = 'ACTIVE' AND
        ks.ACTIVE = 'Y' AND
        ks.KICHEN_STATUS = 'Y'
        {query_conditions}
    """

    if company_name:
        if isinstance(company_name, list):
            query += "AND ("
            for comp in company_name:
                query += f"(cp.COMPANY_NAME LIKE '%{company_name}%') OR "

        else:
            query += f" AND cp.COMPANY_NAME LIKE '%{company_name}%'"

    if restaurant_name:
        query += f" AND ks.KICHEN_NAME LIKE '%{restaurant_name}%'"

    query += " ORDER BY ks.KICHEN_NAME, kb.BASELINE_END_DATE DESC"

    # Execute the query and load the data into a DataFrame
    df = pd.read_sql(query, engine)

    # Remove exact duplicates (rows with the same restaurant_name, company_name, start_date, and end_date)
    df = df.drop_duplicates(
        subset=['restaurant_name', 'company_name', 'start_date', 'end_date'])

    # Close the database connection
    engine.dispose()

    if df.empty:
        print("No baselines found for the specified kitchen and company.")
        return None

    if baseline_selection is not None:
        if baseline_selection < 0 or baseline_selection >= len(df):
            print("Invalid selection.")
            return None
        return df.iloc[[baseline_selection]]

    return df

# Post Baseline Dates, (REPAIRED QUERY, 2024-07-03)
def post_baseline_date(company_name=None, restaurant_name=None, Dummies=True, Expired=False, baseline_selection=None):
    # Establish connection to the database
    engine = create_connection()

    # Construct the SQL query conditions
    query_conditions = ""

    # Aggregate licenses and filter based on the expiration date
    license_aggregation_query = """
        SELECT
            ks.KICHEN_NAME,
            cp.COMPANY_NAME,
            MIN(ca.LICENSE_START_DATE) as LICENSE_START_DATE,
            MAX(ca.LICENSE_EXPIRE_DATE) as LICENSE_EXPIRE_DATE
        FROM
            KITCHEN_STATION ks
        JOIN
            COMPANY_PROFILE cp ON ks.CPN_PF_ID = cp.CPN_PF_ID
        LEFT JOIN
            COMPANY_ACTIVATE ca ON ks.CPN_PF_ID = ca.CPN_PF_ID
        WHERE
            ks.ACTIVE = 'Y' AND
            cp.ACTIVE = 'Y' AND
            cp.COMPANY_STATUS = 'ACTIVE' AND
            ks.KICHEN_STATUS = 'Y'
        GROUP BY
            ks.KICHEN_NAME, cp.COMPANY_NAME
    """

    licenses_df = pd.read_sql_query(license_aggregation_query, engine)

    if Dummies or Expired:
        conditions = []
        if Dummies:
            for restaurant, company in trial_kitchens + demo_kitchens:
                if company:
                    conditions.append(
                        f"(ks.KICHEN_NAME = '{restaurant}' AND cp.COMPANY_NAME = '{company}')")
                else:
                    conditions.append(f"ks.KICHEN_NAME = '{restaurant}'")

            if Expired:
                conditions = [
                    cond for cond in conditions if "trial" in cond or "demo" in cond]

        if conditions:
            conditions_str = " AND NOT (" + " OR ".join(conditions) + ")"
            query_conditions += conditions_str

        if Dummies and not Expired:
            current_date = datetime.now()
            licenses_df = licenses_df[licenses_df['LICENSE_EXPIRE_DATE']
                                      >= current_date]

    query = f"""
    SELECT
        ks.KICHEN_NAME as restaurant_name,
        cp.COMPANY_NAME as company_name,
        MAX(kb.BASELINE_END_DATE) as baseline_end_date
    FROM
        KITCHEN_BASELINE kb
    JOIN
        KITCHEN_STATION ks ON kb.KC_STT_ID = ks.KC_STT_ID
    JOIN
        COMPANY_PROFILE cp ON ks.CPN_PF_ID = cp.CPN_PF_ID
    LEFT JOIN
        COMPANY_ACTIVATE ca ON ks.CPN_PF_ID = ca.CPN_PF_ID
    WHERE
        kb.ACTIVE = 'Y' AND
        cp.ACTIVE = 'Y' AND
        cp.COMPANY_STATUS = 'ACTIVE' AND
        ks.ACTIVE = 'Y' AND
        ks.KICHEN_STATUS = 'Y'
        {query_conditions}
    """

    if company_name:
        query += f" AND cp.COMPANY_NAME = '{company_name}'"

    if restaurant_name:
        query += f" AND ks.KICHEN_NAME = '{restaurant_name}'"

    query += " GROUP BY ks.KICHEN_NAME, cp.COMPANY_NAME ORDER BY ks.KICHEN_NAME, kb.BASELINE_END_DATE DESC"

    # Execute the query and load the data into a DataFrame
    df = pd.read_sql(query, engine)

    # Close the database connection
    engine.dispose()

    if df.empty:
        print("No baselines found for the specified kitchen and company.")
        return None

    if baseline_selection is not None:
        if baseline_selection < 0 or baseline_selection >= len(df):
            print("Invalid selection.")
            return None
        df = df.iloc[[baseline_selection]]

    current_date = datetime.now().date()

    # For all restaurants, calculate the post baseline dates
    df['post_baseline_start_date'] = df['baseline_end_date'] + \
        pd.Timedelta(days=1)
    df['post_baseline_end_date'] = current_date

    return df[['restaurant_name', 'company_name', 'post_baseline_start_date', 'post_baseline_end_date']]

# Function to calculate grams per cover for a specific type (updated)
def g_cover(start_date='2000-01-01', end_date=datetime.now(), company_name=None, restaurant_name=None, shift=None, category=None, food_type=None, CONS=False, SHIFT_ID=True, IGD_FOODTYPE_ID=True, IGD_CATEGORY_ID=True, OPERATION_DATE=True, Dummies=True, Expired=False, grouping='overall'):

    fwcv_comp = get_food_waste_and_covers(start_date=start_date, end_date=end_date, company_name=company_name, restaurant_name=restaurant_name, shift=shift, category=category,
                                          food_type=food_type, CONS=CONS, IGD_CATEGORY_ID=IGD_CATEGORY_ID, IGD_FOODTYPE_ID=IGD_FOODTYPE_ID, SHIFT_ID=SHIFT_ID, Dummies=Dummies, Expired=Expired, OPERATION_DATE=OPERATION_DATE)

    if fwcv_comp.empty:
        return "No data available for the given parameters."

    # Group by the desired time period
    # BETTER AGGREGATION, AND MOST IMPORTANTLY DO THE CALCULATION AFTER AGG
    fwcv_comp['OPERATION_DATE'] = pd.to_datetime(fwcv_comp['OPERATION_DATE'])
    fwcv_comp = fwcv_comp[['COMPANY_NAME',
                           'KICHEN_NAME', 'OPERATION_DATE', 'FW', 'CV']]
    if grouping == 'weekly':
        fwcv_comp = fwcv_comp.groupby(
            [pd.Grouper(key='OPERATION_DATE', freq='W'), 'KICHEN_NAME', 'COMPANY_NAME']).sum()

    elif grouping == 'monthly':
        fwcv_comp = fwcv_comp.groupby(
            [pd.Grouper(key='OPERATION_DATE', freq='M'), 'KICHEN_NAME', 'COMPANY_NAME']).sum()

    elif grouping == 'yearly':
        fwcv_comp = fwcv_comp.groupby(
            [pd.Grouper(key='OPERATION_DATE', freq='Y'), 'KICHEN_NAME', 'COMPANY_NAME']).sum()
    elif grouping == 'overall':
        fwcv_comp = fwcv_comp[['COMPANY_NAME', 'KICHEN_NAME', 'FW', 'CV']].groupby(
            ['KICHEN_NAME', 'COMPANY_NAME']).sum()
    elif grouping == 'daily':
        pass
    else:
        raise ValueError(
            "Invalid grouping specified. Use 'daily', 'weekly', 'monthly', or 'yearly'.")

    fwcv_comp['g_per_cover'] = (fwcv_comp['FW'] / fwcv_comp['CV']) * 1000

    fwcv_comp.rename(columns={'FW': 'Total_Food_Waste',
                     'CV': 'Total_Covers'}, inplace=True)

    return fwcv_comp

def DCON(start_date='2000-01-01', end_date=None, company_name=None, restaurant_name=None,
         TakingBaseline=True, grouping='overall', PerHotel=False, CONS=True, Dummies=True, Expired=False):
    engine = create_connection()  # Ensure this function is defined
    if end_date is None:
        end_date = datetime.now().strftime('%Y-%m-%d')
    cutoff_date = pd.to_datetime('2024-07-01')
    start_date_dt = pd.to_datetime(start_date)
    end_date_dt = pd.to_datetime(end_date)
    conditions = ''

    # Construct the query conditions
    if company_name:
        if isinstance(company_name, list):
            company_conditions = " OR ".join([f"""(cp.COMPANY_NAME = "{comp}")""" for comp in company_name])
            conditions += f"AND ({company_conditions})"
        else:
            conditions += f""" AND cp.COMPANY_NAME LIKE "%{company_name}%" """
    if restaurant_name:
        conditions += f"""AND ks.KICHEN_NAME LIKE "%{restaurant_name}%" """

    if Dummies:
        excluded_kitchens_list = list(excluded_kitchens_set)  # Ensure this variable is defined
        if excluded_kitchens_list:
            exclusion_conditions = " AND NOT (" + " OR ".join(
                [f"""(cp.COMPANY_NAME = "{company}" AND ks.KICHEN_NAME = "{kitchen}")"""
                 for kitchen, company in excluded_kitchens_list]) + ")"
            conditions += exclusion_conditions

    # Aggregate licenses and filter based on the expiration date
    license_aggregation_query = f"""
        SELECT
            ks.KICHEN_NAME,
            cp.COMPANY_NAME,
            MIN(ca.LICENSE_START_DATE) as LICENSE_START_DATE,
            MAX(ca.LICENSE_EXPIRE_DATE) as LICENSE_EXPIRE_DATE
        FROM
            KITCHEN_STATION ks
        RIGHT JOIN
            COMPANY_PROFILE cp ON ks.CPN_PF_ID = cp.CPN_PF_ID
        LEFT JOIN
            COMPANY_ACTIVATE ca ON ks.CPN_PF_ID = ca.CPN_PF_ID
        WHERE
            ks.ACTIVE = 'Y' AND
            cp.ACTIVE = 'Y' AND
            cp.COMPANY_STATUS = 'ACTIVE' AND
            ks.KICHEN_STATUS = 'Y'
        GROUP BY
            ks.KICHEN_NAME, cp.COMPANY_NAME
    """
    licenses_df = pd.read_sql_query(license_aggregation_query, engine)

    # Define the consistency check
    if CONS:
        consistency_str = "kfw.COMPLETE='Y' AND "
        join_cover = ""
    else:
        consistency_str = """
            kc.ACTIVE='Y' AND kfw.ACTIVE = 'Y' AND
            (NOT(kc.AMOUNT IS NULL) OR ks.PRODUCTION_KITCHEN_FLAG='Y') AND
            cp.COMPANY_STATUS  = 'ACTIVE' AND
            cp.ACTIVE='Y' AND
            ks.KICHEN_STATUS = 'Y' AND
            ks.ACTIVE = 'Y' AND
            (kc.ACTIVE='Y' OR kc.ACTIVE IS NULL) AND
            (cs.ACTIVE IS NULL OR cs.ACTIVE='N') AND
            ((kfw.SHIFT_ID='BREAKFAST' AND kos.BREAKFAST='Y') OR
             (kfw.SHIFT_ID='LUNCH' AND kos.LUNCH='Y') OR
             (kfw.SHIFT_ID='DINNER' AND kos.DINNER='Y') OR
             (kfw.SHIFT_ID='BRUNCH' AND kos.BRUNCH='Y') OR
             (kfw.SHIFT_ID='AFTERNOON_TEA' AND kos.AFTERNOON_TEA='Y')) AND
        """
        join_cover = """
            LEFT JOIN KITCHEN_COVER kc ON kc.OPERATION_DATE=kfw.OPERATION_DATE AND kc.SHIFT_ID=kfw.SHIFT_ID AND kc.KC_STT_ID=ks.KC_STT_ID
            LEFT JOIN KITCHEN_SHIFT_CLOSE cs ON cs.KC_STT_ID = ks.KC_STT_ID AND cs.SHIFT_ID = kfw.SHIFT_ID AND cs.CLOSE_DATE = kfw.OPERATION_DATE
            JOIN KITCHEN_OPERATION_SHIFT kos ON kos.KC_STT_ID = ks.KC_STT_ID AND kos.DAY_OF_WEEK=UPPER(DAYNAME(kfw.OPERATION_DATE)) AND kos.OPERATION_SHIFT_TYPE='SHIFT_MAIN'
        """

    # Queries
    firstdate_query = f"""
        SELECT
            cp.COMPANY_NAME,
            ks.KICHEN_NAME,
            MIN(kb.BASELINE_END_DATE) as FirstDate,
            COUNTRY_CODE
        FROM
            COMPANY_PROFILE cp
        JOIN
            KITCHEN_STATION ks ON cp.CPN_PF_ID = ks.CPN_PF_ID
        JOIN
            KITCHEN_BASELINE kb ON ks.KC_STT_ID = kb.KC_STT_ID
        LEFT JOIN
            COMPANY_ACTIVATE ca ON ks.CPN_PF_ID = ca.CPN_PF_ID
        WHERE
            kb.ACTIVE = 'Y' AND
            cp.COMPANY_STATUS = 'ACTIVE' AND
            cp.ACTIVE = 'Y'
            {conditions} AND
            ks.KICHEN_STATUS = 'Y' AND
            ks.ACTIVE = 'Y'
        GROUP BY
            cp.COMPANY_NAME, ks.KICHEN_NAME
        ORDER BY
            cp.COMPANY_NAME, ks.KICHEN_NAME;
    """
    firstdate = pd.read_sql_query(firstdate_query, engine)

    # Load data common to both methods
    opening_shift_query = f"""
        SELECT
            cp.COMPANY_NAME,
            ks.KICHEN_NAME,
            kos.DAY_OF_WEEK,
            kos.BREAKFAST,
            kos.BRUNCH,
            kos.LUNCH,
            kos.AFTERNOON_TEA,
            kos.DINNER
        FROM
            KITCHEN_OPERATION_SHIFT kos
        JOIN
            COMPANY_PROFILE cp ON kos.CPN_PF_ID = cp.CPN_PF_ID
        JOIN
            KITCHEN_STATION ks ON kos.KC_STT_ID = ks.KC_STT_ID
        WHERE
            kos.ACTIVE = 'Y' AND
            cp.COMPANY_STATUS = 'ACTIVE'
            {conditions} AND
            cp.ACTIVE = 'Y' AND
            ks.KICHEN_STATUS = 'Y' AND
            ks.ACTIVE ='Y' AND
            kos.OPERATION_SHIFT_TYPE = 'SHIFT_MAIN'
        ORDER BY
            cp.COMPANY_NAME, ks.KICHEN_NAME;
    """
    opening_shifts = pd.read_sql_query(opening_shift_query, engine).drop_duplicates()
    if opening_shifts.empty:
        print("opening_shifts DataFrame is empty.")
        return pd.DataFrame()

    closed_shifts_query = f"""
        SELECT
            cp.COMPANY_NAME,
            ks.KICHEN_NAME,
            ksc.CLOSE_DATE as OPERATION_DATE,
            ksc.SHIFT_ID
        FROM
            KITCHEN_SHIFT_CLOSE ksc
        JOIN
            COMPANY_PROFILE cp ON cp.CPN_PF_ID = ksc.CPN_PF_ID
        JOIN
            KITCHEN_STATION ks ON ks.KC_STT_ID = ksc.KC_STT_ID
        WHERE
            ksc.ACTIVE = 'Y' AND
            cp.COMPANY_STATUS  = 'ACTIVE' AND
            ks.KICHEN_STATUS = 'Y' AND
            ks.ACTIVE = 'Y'
            {conditions} AND
            (ksc.CLOSE_DATE BETWEEN '{start_date}' AND '{end_date}')
        ORDER BY
            cp.COMPANY_NAME, ks.KICHEN_NAME, ksc.CLOSE_DATE;
    """
    closed_shifts = pd.read_sql_query(closed_shifts_query, engine).drop_duplicates()
    closed_shifts['OPERATION_DATE'] = pd.to_datetime(closed_shifts['OPERATION_DATE'])

    # Choose the appropriate data query based on cutoff_date
    if end_date_dt < cutoff_date:
        # Old method query
        data_query = f"""
            SELECT
                kfw.OPERATION_DATE,
                cp.COMPANY_NAME,
                ks.KICHEN_NAME,
                kfw.SHIFT_ID,
                SUM(kfw.AMOUNT) as FW,
                SUM(kc_main.AMOUNT) as CV
            FROM
                KITCHEN_FOOD_WASTE kfw
            JOIN
                KITCHEN_STATION ks ON kfw.KC_STT_ID = ks.KC_STT_ID
            LEFT JOIN
                KITCHEN_COVER kc_main ON kc_main.KC_STT_ID = ks.KC_STT_ID AND kc_main.SHIFT_ID = kfw.SHIFT_ID AND kc_main.OPERATION_DATE = kfw.OPERATION_DATE
            JOIN
                COMPANY_PROFILE cp ON ks.CPN_PF_ID = cp.CPN_PF_ID
            {join_cover}
            WHERE
                kfw.ACTIVE = 'Y'
                {conditions} AND
                {consistency_str}
                kfw.OPERATION_DATE BETWEEN '{start_date}' AND '{end_date}'
            GROUP BY
                cp.COMPANY_NAME, ks.KICHEN_NAME, kfw.OPERATION_DATE, kfw.SHIFT_ID;
        """
    elif start_date_dt >= cutoff_date:
        # New method query
        data_query = f"""
            SELECT
                DATE(kfw.OPERATION_DATE) as OPERATION_DATE,
                cp.COMPANY_NAME,
                ks.KICHEN_NAME,
                kfw.SHIFT_ID
            FROM
                KITCHEN_FOOD_WASTE kfw
            JOIN
                KITCHEN_STATION ks ON kfw.KC_STT_ID = ks.KC_STT_ID
            JOIN
                COMPANY_PROFILE cp ON ks.CPN_PF_ID = cp.CPN_PF_ID
            WHERE
                kfw.ACTIVE = 'Y'
                {conditions} AND
                kfw.COMPLETE='Y' AND
                kfw.OPERATION_DATE BETWEEN '{start_date}' AND '{end_date}'
            GROUP BY
                cp.COMPANY_NAME, ks.KICHEN_NAME, kfw.OPERATION_DATE, kfw.SHIFT_ID;
        """
    else:
        # Handle date range that spans the cutoff_date
        data_query_before = f"""
            SELECT
                kfw.OPERATION_DATE,
                cp.COMPANY_NAME,
                ks.KICHEN_NAME,
                kfw.SHIFT_ID,
                SUM(kfw.AMOUNT) as FW,
                SUM(kc_main.AMOUNT) as CV
            FROM
                KITCHEN_FOOD_WASTE kfw
            JOIN
                KITCHEN_STATION ks ON kfw.KC_STT_ID = ks.KC_STT_ID
            LEFT JOIN
                KITCHEN_COVER kc_main ON kc_main.KC_STT_ID = ks.KC_STT_ID AND kc_main.SHIFT_ID = kfw.SHIFT_ID AND kc_main.OPERATION_DATE = kfw.OPERATION_DATE
            JOIN
                COMPANY_PROFILE cp ON ks.CPN_PF_ID = cp.CPN_PF_ID
            {join_cover}
            WHERE
                kfw.ACTIVE = 'Y'
                {conditions} AND
                {consistency_str}
                kfw.OPERATION_DATE BETWEEN '{start_date}' AND '{(cutoff_date - timedelta(days=1)).strftime('%Y-%m-%d')}'
            GROUP BY
                cp.COMPANY_NAME, ks.KICHEN_NAME, kfw.OPERATION_DATE, kfw.SHIFT_ID;
        """
        data_query_after = f"""
            SELECT
                DATE(kfw.OPERATION_DATE) as OPERATION_DATE,
                cp.COMPANY_NAME,
                ks.KICHEN_NAME,
                kfw.SHIFT_ID
            FROM
                KITCHEN_FOOD_WASTE kfw
            JOIN
                KITCHEN_STATION ks ON kfw.KC_STT_ID = ks.KC_STT_ID
            JOIN
                COMPANY_PROFILE cp ON ks.CPN_PF_ID = cp.CPN_PF_ID
            WHERE
                kfw.ACTIVE = 'Y'
                {conditions} AND
                kfw.COMPLETE='Y' AND
                kfw.OPERATION_DATE BETWEEN '{cutoff_date.strftime('%Y-%m-%d')}' AND '{end_date}'
            GROUP BY
                cp.COMPANY_NAME, ks.KICHEN_NAME, kfw.OPERATION_DATE, kfw.SHIFT_ID;
        """
        data_before = pd.read_sql_query(data_query_before, engine)
        data_after = pd.read_sql_query(data_query_after, engine)
        data = pd.concat([data_before, data_after], ignore_index=True)
    if 'data' not in locals():
        data = pd.read_sql_query(data_query, engine)
    if data.empty:
        print("Data DataFrame is empty.")
        return pd.DataFrame()

    # Convert dates to datetime
    data['OPERATION_DATE'] = pd.to_datetime(data['OPERATION_DATE'])
    closed_shifts['OPERATION_DATE'] = pd.to_datetime(closed_shifts['OPERATION_DATE'])

    # Merge baseline data and handle missing FirstDate
    for df in [data, closed_shifts]:
        df = df.merge(firstdate, on=['COMPANY_NAME', 'KICHEN_NAME'], how='left')
        df['FirstDate'] = pd.to_datetime(df['FirstDate'])
        df['FirstDate'] = df['FirstDate'].fillna(start_date_dt)
        df = df[df['OPERATION_DATE'] > df['FirstDate']]
        df.drop(['FirstDate', 'COUNTRY_CODE'], axis=1, inplace=True)

    # Generate full schedule
    all_dates = pd.date_range(start=start_date, end=end_date)
    days_of_week = all_dates.day_name().str.upper()
    dates_df = pd.DataFrame({'OPERATION_DATE': all_dates, 'DAY_OF_WEEK': days_of_week})
    opening_shifts['DAY_OF_WEEK'] = opening_shifts['DAY_OF_WEEK'].str.upper()

    # Merge to get possible shifts
    schedule = dates_df.merge(opening_shifts, on='DAY_OF_WEEK', how='left')
    schedule_melted = schedule.melt(
        id_vars=['COMPANY_NAME', 'KICHEN_NAME', 'OPERATION_DATE', 'DAY_OF_WEEK'],
        value_vars=['BREAKFAST', 'BRUNCH', 'LUNCH', 'AFTERNOON_TEA', 'DINNER'],
        var_name='SHIFT_ID',
        value_name='SHIFT_OPEN'
    )
    full_schedule_df = schedule_melted[schedule_melted['SHIFT_OPEN'] == 'Y'].drop('SHIFT_OPEN', axis=1)
    full_schedule_df = full_schedule_df.drop_duplicates(subset=['COMPANY_NAME', 'KICHEN_NAME', 'OPERATION_DATE', 'SHIFT_ID'])

    # Merge full_schedule_df with firstdate and filter based on FirstDate
    full_schedule_df = full_schedule_df.merge(firstdate[['COMPANY_NAME', 'KICHEN_NAME', 'FirstDate']], on=['COMPANY_NAME', 'KICHEN_NAME'], how='left')
    full_schedule_df['FirstDate'] = pd.to_datetime(full_schedule_df['FirstDate'])
    full_schedule_df['FirstDate'] = full_schedule_df['FirstDate'].fillna(start_date_dt)
    full_schedule_df = full_schedule_df[full_schedule_df['OPERATION_DATE'] > full_schedule_df['FirstDate']]
    full_schedule_df = full_schedule_df.drop(['FirstDate'], axis=1)

    # **Include shifts from data that are not in full_schedule_df**
    data_shifts = data[['COMPANY_NAME', 'KICHEN_NAME', 'OPERATION_DATE', 'SHIFT_ID']].drop_duplicates()
    full_schedule_df = pd.concat([full_schedule_df, data_shifts], ignore_index=True).drop_duplicates(subset=['COMPANY_NAME', 'KICHEN_NAME', 'OPERATION_DATE', 'SHIFT_ID'])

    # Add grouping columns
    if grouping in ['monthly', 'yearly', 'weekly']:
        for df in [data, closed_shifts, full_schedule_df]:
            df['YEAR'] = df['OPERATION_DATE'].dt.year
            if grouping == 'monthly':
                df['MONTH'] = df['OPERATION_DATE'].dt.month
            elif grouping == 'weekly':
                df['WEEK_START_DATE'] = df['OPERATION_DATE'] - pd.to_timedelta(df['OPERATION_DATE'].dt.weekday, unit='D')

    # Prepare closed_shifts DataFrame
    closed_shifts['DAY_OF_WEEK'] = closed_shifts['OPERATION_DATE'].dt.day_name().str.upper()
    opening_shifts_melted = opening_shifts.melt(
        id_vars=['COMPANY_NAME', 'KICHEN_NAME', 'DAY_OF_WEEK'],
        value_vars=['BREAKFAST', 'BRUNCH', 'LUNCH', 'AFTERNOON_TEA', 'DINNER'],
        var_name='SHIFT_ID',
        value_name='SHIFT_STATUS'
    )
    merged_closed_shifts = closed_shifts.merge(
        opening_shifts_melted,
        on=['COMPANY_NAME', 'KICHEN_NAME', 'DAY_OF_WEEK', 'SHIFT_ID'],
        how='left'
    )
    merged_closed_shifts['IS_REDUNDANT'] = merged_closed_shifts['SHIFT_STATUS'].apply(
        lambda x: 'Y' if x == 'N' or pd.isna(x) else 'N'
    )
    closed_shifts = merged_closed_shifts[merged_closed_shifts['IS_REDUNDANT'] == 'N']

    # Calculations
    data['COMP_SHIFTS'] = 1
    closed_shifts['CLOSED_SHIFTS'] = 1
    full_schedule_df['TOTAL_SHIFTS'] = 1

    # Define grouping columns
    group_columns = ['COMPANY_NAME', 'KICHEN_NAME']
    if grouping == 'monthly':
        group_columns += ['YEAR', 'MONTH']
    elif grouping == 'weekly':
        group_columns += ['WEEK_START_DATE']
    elif grouping == 'yearly':
        group_columns += ['YEAR']
    elif grouping == 'daily':
        group_columns += ['OPERATION_DATE']

    # Group data
    total_shifts = full_schedule_df.groupby(group_columns).agg({'TOTAL_SHIFTS': 'count'}).reset_index()
    comp_shifts = data.groupby(group_columns).agg({'COMP_SHIFTS': 'count'}).reset_index()
    closed_shifts_count = closed_shifts.groupby(group_columns).agg({'CLOSED_SHIFTS': 'count'}).reset_index()

    # Merge data
    dcon_data = total_shifts.merge(comp_shifts, on=group_columns, how='left')
    dcon_data = dcon_data.merge(closed_shifts_count, on=group_columns, how='left')
    dcon_data['COMP_SHIFTS'] = dcon_data['COMP_SHIFTS'].fillna(0)
    dcon_data['CLOSED_SHIFTS'] = dcon_data['CLOSED_SHIFTS'].fillna(0)
    dcon_data['CONSISTENCY'] = (dcon_data['COMP_SHIFTS'] / (dcon_data['TOTAL_SHIFTS'] - dcon_data['CLOSED_SHIFTS'])).round(2)
    dcon_data['CONSISTENCY'] = dcon_data['CONSISTENCY'].replace([np.inf, -np.inf], 0).fillna(0)

    # Add START_DATE and END_DATE
    bounds = full_schedule_df.groupby(['COMPANY_NAME', 'KICHEN_NAME']).agg(
        START_DATE=('OPERATION_DATE', 'min'),
        END_DATE=('OPERATION_DATE', 'max')
    ).reset_index()
    dcon_data = dcon_data.merge(bounds, on=['COMPANY_NAME', 'KICHEN_NAME'], how='left')

    # Prepare final data based on grouping
    if grouping == 'monthly':
        dcon_data['OPERATION_DATE'] = pd.to_datetime(dcon_data[['YEAR', 'MONTH']].assign(DAY=1))
        dcon_data = dcon_data.drop(columns=['YEAR', 'MONTH'])
    elif grouping == 'weekly':
        pass  # WEEK_START_DATE is already in the DataFrame
    elif grouping == 'yearly':
        dcon_data['OPERATION_DATE'] = pd.to_datetime(dcon_data['YEAR'], format='%Y')
        dcon_data = dcon_data.drop(columns=['YEAR'])
    elif grouping == 'daily':
        pass  # OPERATION_DATE is already in the DataFrame
    elif grouping == 'overall':
        pass  # No additional adjustments needed
    else:
        raise ValueError(f"Invalid grouping value: {grouping}. Choose from 'daily', 'weekly', 'monthly', 'yearly', 'overall'.")

    # Select columns to return
    columns_to_select = ['COMPANY_NAME', 'KICHEN_NAME']
    if grouping in ['monthly', 'daily', 'yearly']:
        columns_to_select.append('OPERATION_DATE')
    elif grouping == 'weekly':
        columns_to_select.append('WEEK_START_DATE')
    columns_to_select += ['CONSISTENCY', 'TOTAL_SHIFTS', 'COMP_SHIFTS', 'CLOSED_SHIFTS']
    if grouping == 'overall':
        columns_to_select += ['START_DATE', 'END_DATE']

    if PerHotel:
        group_cols = ['COMPANY_NAME']
        if 'WEEK_START_DATE' in dcon_data.columns:
            group_cols.append('WEEK_START_DATE')
        elif 'OPERATION_DATE' in dcon_data.columns:
            group_cols.append('OPERATION_DATE')
        dcon_data = dcon_data.groupby(group_cols).sum().reset_index()
        dcon_data['CONSISTENCY'] = (dcon_data['COMP_SHIFTS'] / (dcon_data['TOTAL_SHIFTS'] - dcon_data['CLOSED_SHIFTS'])).round(2)
        columns_to_select = [col for col in columns_to_select if col != 'KICHEN_NAME']

    dcon_data = dcon_data[columns_to_select]
    dcon_data = dcon_data.sort_values(by=columns_to_select[:2])

    # Merge with licenses_df if provided
    if licenses_df is not None:
        dcon_data = dcon_data.merge(licenses_df, on=['COMPANY_NAME', 'KICHEN_NAME'], how='inner')

    return dcon_data

# Savings
def get_savings(start_date=None, end_date=None, CONS=False, company_name=None, restaurant_name=None, Baseline_Entry=None, shift=None, category=None, foodtype=None, Dummies=True, with_old_calc=False, MergeKitchen=False, MergeComp=False, Expired=False):
    load_dotenv()

    # Prepare query conditions
    old_new_condition = "kfw.COMPLETE='Y'"
    if with_old_calc:
        old_new_condition = """((kfw.COMPLETE='Y' AND DATE(kfw.OPERATION_DATE)>='2024-07-01') OR 
            (DATE(kfw.OPERATION_DATE)<'2024-07-01' AND 
            cp.COMPANY_STATUS='ACTIVE' AND
            cp.ACTIVE='Y' AND
            ks.KICHEN_STATUS='Y' AND
            ks.ACTIVE='Y' AND
            (kc.ACTIVE='Y' OR kc.ACTIVE IS NULL) AND
            (cs.ACTIVE IS NULL OR cs.ACTIVE='N') AND
            ((kfw.SHIFT_ID='BREAKFAST' AND kos.BREAKFAST='Y') OR 
             (kfw.SHIFT_ID='LUNCH' AND kos.LUNCH='Y') OR 
             (kfw.SHIFT_ID='DINNER' AND kos.DINNER='Y') OR 
             (kfw.SHIFT_ID='BRUNCH' AND kos.BRUNCH='Y') OR 
             (kfw.SHIFT_ID='AFTERNOON_TEA' AND kos.AFTERNOON_TEA='Y'))))"""

    start_date = start_date or '2000-01-01'
    end_date = end_date or datetime.now().strftime('%Y-%m-%d')
    if Baseline_Entry:
        Baseline_Entry = (pd.to_datetime(Baseline_Entry[0]), pd.to_datetime(Baseline_Entry[1]))

    engine = create_connection()
    kitchen_column = '' if MergeKitchen else ', ks.KICHEN_NAME'

    query_conditions = []

    # Handle Dummies
    Dummies_String = ""
    if Dummies:
        conditions = []
        for restaurant, company in trial_kitchens + demo_kitchens:
            if company:
                conditions.append(f"(ks.KICHEN_NAME = '{restaurant}' AND cp.COMPANY_NAME = '{company}')")
            else:
                conditions.append(f"ks.KICHEN_NAME = '{restaurant}'")
        if conditions:
            Dummies_String = "NOT (" + " OR ".join(conditions) + ")"

    # Build query conditions
    if CONS:
        query_conditions.append("kfw.COMPLETE='Y'")
    if company_name:
        query_conditions.append(f"cp.COMPANY_NAME LIKE '%{company_name}%'")
    if restaurant_name:
        query_conditions.append(f"ks.KICHEN_NAME LIKE '%{restaurant_name}%'")
    if shift and shift != 'ALL':
        query_conditions.append(f'kfw.SHIFT_ID="{shift.upper()}"')
    if category and category != 'ALL':
        query_conditions.append(f'kfw.IGD_CATEGORY_ID="{category.upper()}"')
    if foodtype and foodtype != 'ALL':
        query_conditions.append(f'kfw.IGD_FOODTYPE_ID="{foodtype.upper()}"')
    if Expired:
        query_conditions.append('ca.LICENSE_EXPIRE_DATE >= CURDATE()')

    # Combine all conditions
    all_conditions = ["kfw.ACTIVE = 'Y'"]
    if old_new_condition:
        all_conditions.append(f"({old_new_condition})")
    if Dummies_String:
        all_conditions.append(Dummies_String)
    if query_conditions:
        all_conditions.extend(query_conditions)
    all_conditions.append("kc.ACTIVE='Y'")
    all_conditions.append(f"kfw.OPERATION_DATE BETWEEN '{start_date}' AND '{end_date}'")

    where_clause = ' AND '.join(all_conditions)

    kitchen_join = "LEFT JOIN lightblue.COMPANY_ACTIVATE ca ON ks.CPN_PF_ID = ca.CPN_PF_ID" if Expired else ''

    # SQL Query for main data
    fw_cv_query = f"""
    SELECT
        DATE(kfw.OPERATION_DATE) as OPERATION_DATE,
        cp.COMPANY_NAME,
        ks.KICHEN_NAME,
        kfw.SHIFT_ID,
        kfw.IGD_CATEGORY_ID,
        SUM(kfw.AMOUNT) as FW,
        kc.AMOUNT as CV
    FROM
        lightblue.KITCHEN_FOOD_WASTE kfw
    JOIN
        lightblue.KITCHEN_STATION ks ON kfw.KC_STT_ID = ks.KC_STT_ID
    JOIN
        lightblue.COMPANY_PROFILE cp ON ks.CPN_PF_ID = cp.CPN_PF_ID
    LEFT JOIN
        KITCHEN_COVER kc ON kc.KC_STT_ID = ks.KC_STT_ID AND kc.SHIFT_ID = kfw.SHIFT_ID AND kc.OPERATION_DATE = kfw.OPERATION_DATE
    LEFT JOIN
        lightblue.KITCHEN_SHIFT_CLOSE cs ON cs.KC_STT_ID = ks.KC_STT_ID AND cs.SHIFT_ID = kfw.SHIFT_ID AND cs.CLOSE_DATE = kfw.OPERATION_DATE
    {kitchen_join}
    JOIN
        lightblue.KITCHEN_OPERATION_SHIFT kos ON kos.KC_STT_ID=ks.KC_STT_ID AND kos.DAY_OF_WEEK=UPPER(DAYNAME(kfw.OPERATION_DATE)) AND kos.OPERATION_SHIFT_TYPE='SHIFT_MAIN'
    WHERE
        {where_clause}
    GROUP BY
        cp.COMPANY_NAME{kitchen_column}, kfw.OPERATION_DATE, kfw.SHIFT_ID"""

    # SQL Query for baseline data
    # Adjust conditions for baseline query if needed
    where_clause_baseline = ' AND '.join(cond for cond in all_conditions if not cond.startswith("kfw.OPERATION_DATE BETWEEN"))

    fw_cv_b_query = f"""
    SELECT
        DATE(kfw.OPERATION_DATE) as OPERATION_DATE,
        cp.COMPANY_NAME,
        ks.KICHEN_NAME,
        kfw.SHIFT_ID,
        kfw.IGD_CATEGORY_ID,
        SUM(kfw.AMOUNT) as FW,
        kc.AMOUNT as CV
    FROM
        lightblue.KITCHEN_FOOD_WASTE kfw
    JOIN
        lightblue.KITCHEN_STATION ks ON kfw.KC_STT_ID = ks.KC_STT_ID
    JOIN
        lightblue.COMPANY_PROFILE cp ON ks.CPN_PF_ID = cp.CPN_PF_ID
    LEFT JOIN
        KITCHEN_COVER kc ON kc.KC_STT_ID = ks.KC_STT_ID AND kc.SHIFT_ID = kfw.SHIFT_ID AND kc.OPERATION_DATE = kfw.OPERATION_DATE
    LEFT JOIN
        lightblue.KITCHEN_SHIFT_CLOSE cs ON cs.KC_STT_ID = ks.KC_STT_ID AND cs.SHIFT_ID = kfw.SHIFT_ID AND cs.CLOSE_DATE = kfw.OPERATION_DATE
    {kitchen_join}
    JOIN
        lightblue.KITCHEN_OPERATION_SHIFT kos ON kos.KC_STT_ID=ks.KC_STT_ID AND kos.DAY_OF_WEEK=UPPER(DAYNAME(kfw.OPERATION_DATE)) AND kos.OPERATION_SHIFT_TYPE='SHIFT_MAIN'
    WHERE
        {where_clause_baseline}
    GROUP BY
        cp.COMPANY_NAME{kitchen_column}, kfw.OPERATION_DATE, kfw.SHIFT_ID"""

    # Fetch data from database
    fw_cv_comp = pd.read_sql_query(fw_cv_query, engine)
    fw_cv_comp_baseline = pd.read_sql_query(fw_cv_b_query, engine)

    # Get baseline data
    baseline_data = baseline_date(company_name=company_name, restaurant_name=restaurant_name, Dummies=Dummies)
    if baseline_data is None or baseline_data.empty:
        print("No baselines found for the specified kitchen and company.")
        return None

    if 'COUNTRY_CODE' not in baseline_data.columns:
        baseline_data['COUNTRY_CODE'] = 'Unknown'

    if MergeKitchen:
        fw_cv_comp_baseline['KICHEN_NAME'] = 'Merged'
        fw_cv_comp['KICHEN_NAME'] = 'Merged'
        baseline_data['restaurant_name'] = 'Merged'
    if MergeComp:
        fw_cv_comp_baseline['COMPANY_NAME'] = company_name
        fw_cv_comp['COMPANY_NAME'] = company_name
        baseline_data['company_name'] = company_name

    # Ensure date columns are datetime
    fw_cv_comp_baseline['OPERATION_DATE'] = pd.to_datetime(fw_cv_comp_baseline['OPERATION_DATE'])
    fw_cv_comp['OPERATION_DATE'] = pd.to_datetime(fw_cv_comp['OPERATION_DATE'])
    baseline_data['start_date'] = pd.to_datetime(baseline_data['start_date'])
    baseline_data['end_date'] = pd.to_datetime(baseline_data['end_date'])

    # Rename columns in baseline_data to match
    baseline_data.rename(columns={'company_name': 'COMPANY_NAME', 'restaurant_name': 'KICHEN_NAME'}, inplace=True)

    # Merge fw_cv_comp_baseline with baseline_data
    merged_baseline = pd.merge(
        fw_cv_comp_baseline,
        baseline_data[['COMPANY_NAME', 'KICHEN_NAME', 'start_date', 'end_date']],
        on=['COMPANY_NAME', 'KICHEN_NAME'],
        how='inner'
    )

    # Filter within baseline date ranges
    merged_baseline = merged_baseline[
        (merged_baseline['OPERATION_DATE'] >= merged_baseline['start_date']) &
        (merged_baseline['OPERATION_DATE'] <= merged_baseline['end_date'])
    ]

    # Compute baseline FWCV
    baseline_fwcv = merged_baseline.groupby(['COMPANY_NAME', 'KICHEN_NAME']).agg({'FW': 'sum', 'CV': 'sum'}).reset_index()
    baseline_fwcv['FWCV'] = baseline_fwcv['FW'] / baseline_fwcv['CV']

    # Compute baseline FWCV per shift
    baseline_fwcv_shifts = merged_baseline.groupby(['COMPANY_NAME', 'KICHEN_NAME', 'SHIFT_ID']).agg({'FW': 'sum', 'CV': 'sum'}).reset_index()
    baseline_fwcv_shifts['FWCV_shift'] = baseline_fwcv_shifts['FW'] / baseline_fwcv_shifts['CV']

    # Pivot shifts
    baseline_fwcv_shifts_pivot = baseline_fwcv_shifts.pivot_table(
        index=['COMPANY_NAME', 'KICHEN_NAME'],
        columns='SHIFT_ID',
        values='FWCV_shift'
    ).reset_index()

    # Merge shifts with baseline_fwcv
    baseline_fwcv = baseline_fwcv.merge(baseline_fwcv_shifts_pivot, on=['COMPANY_NAME', 'KICHEN_NAME'], how='left')

    # Merge computed FWCVs back into baseline_data
    baseline_data = baseline_data.merge(baseline_fwcv, on=['COMPANY_NAME', 'KICHEN_NAME'], how='left')

    # Remove baselines where FWCV is NaN
    baseline_data = baseline_data[baseline_data['FWCV'].notna()]

    # Merge fw_cv_comp with baseline_data
    merged_fwcv = pd.merge(
        fw_cv_comp,
        baseline_data[['COMPANY_NAME', 'KICHEN_NAME', 'end_date', 'FWCV', 'start_date', 'COUNTRY_CODE']],
        on=['COMPANY_NAME', 'KICHEN_NAME'],
        how='inner'
    )

    # Filter for post-baseline period
    merged_fwcv = merged_fwcv[merged_fwcv['OPERATION_DATE'] > merged_fwcv['end_date']]

    # Also filter within start_date and end_date if specified
    if start_date:
        merged_fwcv = merged_fwcv[merged_fwcv['OPERATION_DATE'] >= pd.to_datetime(start_date)]
    if end_date:
        merged_fwcv = merged_fwcv[merged_fwcv['OPERATION_DATE'] <= pd.to_datetime(end_date)]

    # Compute post-baseline FWCV
    post_baseline_fwcv = merged_fwcv.groupby(['COMPANY_NAME', 'KICHEN_NAME']).agg({'FW': 'sum', 'CV': 'sum'}).reset_index()
    post_baseline_fwcv['FWCV_post'] = post_baseline_fwcv['FW'] / post_baseline_fwcv['CV']

    # Calculate savings
    savings_data = post_baseline_fwcv.merge(
        baseline_data[['COMPANY_NAME', 'KICHEN_NAME', 'FWCV', 'start_date', 'end_date', 'COUNTRY_CODE']],
        on=['COMPANY_NAME', 'KICHEN_NAME'],
        how='left'
    )
    savings_data['FWCV_variation'] = ((savings_data['FWCV_post'] - savings_data['FWCV']) / savings_data['FWCV']) * 100  # in percentage
    savings_data['saved food (in kg)'] = (savings_data['FWCV'] - savings_data['FWCV_post']) * savings_data['CV'] / 1000  # Convert grams to kg

    # Additional metrics
    daily_fw = merged_fwcv.groupby(['COMPANY_NAME', 'KICHEN_NAME', 'OPERATION_DATE']).agg({'FW': 'sum'}).reset_index()
    daily_fw_avg = daily_fw.groupby(['COMPANY_NAME', 'KICHEN_NAME']).agg({'FW': 'mean'}).reset_index()
    daily_fw_avg.rename(columns={'FW': 'daily kg wasted'}, inplace=True)
    daily_fw_avg['daily kg wasted'] = daily_fw_avg['daily kg wasted'] / 1000  # Convert grams to kg

    total_fw = merged_fwcv.groupby(['COMPANY_NAME', 'KICHEN_NAME']).agg({'FW': 'sum'}).reset_index()
    total_fw.rename(columns={'FW': 'kg wasted'}, inplace=True)
    total_fw['kg wasted'] = total_fw['kg wasted'] / 1000  # Convert grams to kg

    total_cv = merged_fwcv.groupby(['COMPANY_NAME', 'KICHEN_NAME']).agg({'CV': 'sum'}).reset_index()
    total_cv.rename(columns={'CV': 'Number of Covers'}, inplace=True)

    # Merge additional metrics
    savings_data = savings_data.merge(daily_fw_avg, on=['COMPANY_NAME', 'KICHEN_NAME'], how='left')
    savings_data = savings_data.merge(total_fw, on=['COMPANY_NAME', 'KICHEN_NAME'], how='left')
    savings_data = savings_data.merge(total_cv, on=['COMPANY_NAME', 'KICHEN_NAME'], how='left')

    # Prepare final results
    results = savings_data[['COMPANY_NAME', 'KICHEN_NAME', 'COUNTRY_CODE', 'FWCV', 'start_date', 'end_date',
                            'FWCV_post', 'FWCV_variation', 'saved food (in kg)', 'daily kg wasted', 'kg wasted', 'Number of Covers']]

    results.rename(columns={
        'FWCV': 'g/cover during Baseline 1',
        'start_date': 'BL 1 Start Date',
        'end_date': 'BL 1 End Date',
        'FWCV_post': 'g/cover PBL',
        'FWCV_variation': 'FWCV variation (%)',  # Corrected column name
    }, inplace=True)


    # Multiply g/cover values by 1000 to convert to grams (assuming FWCV was in kg/cover)
    results['g/cover during Baseline 1'] = results['g/cover during Baseline 1'] * 1000  # Convert kg to grams
    results['g/cover PBL'] = results['g/cover PBL'] * 1000  # Convert kg to grams

    # Round numerical values for better readability
    results['g/cover during Baseline 1'] = results['g/cover during Baseline 1'].round(2)
    results['g/cover PBL'] = results['g/cover PBL'].round(2)
    results['FWCV variation (%)'] = results['FWCV variation (%)'].round(2)
    results['saved food (in kg)'] = results['saved food (in kg)'].round(2)
    results['daily kg wasted'] = results['daily kg wasted'].round(2)
    results['kg wasted'] = results['kg wasted'].round(2)
    results['Number of Covers'] = results['Number of Covers'].astype(int)

    return results