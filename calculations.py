from dotenv import load_dotenv
from sqlalchemy import create_engine
import os
import pandas as pd
from datetime import timedelta, datetime
import urllib.parse 
import sqlalchemy
import numpy as np
import openpyxl

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


old_kitchens = [
    ("Balcony", "BEIGH"),
    ("Made For You", "BEIGH"),
    ("Le Saint Germain", "Le Louvre Hotel & Spa (old)"),
    ("Osteria", "Hyatt Regency Danang Osteria"),
    ("Pool House", "Hyatt Regency Danang Pool House"),
    ("Regency Club", "Hyatt Regency Danang Regency Club"),
    ("Staff Canteen", "Hyatt Regency Danang Staff Canteen")
]

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
    ("Mamadoo", "Mamadoo Company Limited")
]

excluded_kitchens_set = set(old_kitchens + trial_kitchens + demo_kitchens)


# Create a connection to the MySQL database
def create_connection():
    connection = None
    try:
        # Construct the connection string
        connection_string = f"mysql+mysqlconnector://{USERNAME}:{PASSWORD}@{HOST}/{DATABASE_NAME}"
        connection = create_engine(connection_string)
        print("Connection to MySQL DB successful")
    except Exception as e:
        print(f"The error '{e}' occurred")
    
    return connection

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
    
    # License aggregation query
    license_aggregation_query = f"""
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
            {query_conditions}
        GROUP BY
            ks.KICHEN_NAME, cp.COMPANY_NAME
    """
    
    licenses_df = pd.read_sql_query(license_aggregation_query, engine)
    
    # Handle Dummies and Expired conditions
    if Dummies or Expired:
        conditions = []
        if Dummies:
            for restaurant, company in old_kitchens + trial_kitchens + demo_kitchens:
                if company:
                    conditions.append(f"(ks.KICHEN_NAME = '{restaurant}' AND cp.COMPANY_NAME = '{company}')")
                else:
                    conditions.append(f"ks.KICHEN_NAME = '{restaurant}'")
            
            if Expired:
                conditions = [cond for cond in conditions if "trial" in cond or "demo" in cond]
        
        elif Expired:
            for restaurant, company in old_kitchens:
                if company:
                    conditions.append(f"(ks.KICHEN_NAME = '{restaurant}' AND cp.COMPANY_NAME = '{company}')")
                else:
                    conditions.append(f"ks.KICHEN_NAME = '{restaurant}'")
        
        if conditions:
            conditions_str = " AND NOT (" + " OR ".join(conditions) + ")"
            query_conditions += conditions_str
        
        # Filter out expired licenses if Dummies is True and Expired is False
        if Dummies and not Expired:
            current_date = datetime.now()
            licenses_df = licenses_df[licenses_df['LICENSE_EXPIRE_DATE'] > current_date]
    email_query = ""
    # Final query with filtered conditions
    if Emails:
        email_query = """,
        cp.WEEKLY_REPORT_MAIL_TO as Mail_To,
        cp.WEEKLY_REPORT_MAIL_CC as Mail_Cc"""
    query = f"""
        SELECT 
            ks.KICHEN_NAME as kitchen_name,
            cp.COMPANY_NAME as company_name,
            MIN(ca.LICENSE_START_DATE) as LICENSE_START_DATE,
            MAX(ca.LICENSE_EXPIRE_DATE) as LICENSE_EXPIRE_DATE{email_query}
        FROM 
            KITCHEN_STATION ks
        JOIN 
            COMPANY_PROFILE cp ON ks.CPN_PF_ID = cp.CPN_PF_ID
        LEFT JOIN
            COMPANY_ACTIVATE ca ON ks.CPN_PF_ID = ca.CPN_PF_ID
        WHERE 
            {query_conditions}
        GROUP BY 
            ks.KICHEN_NAME, cp.COMPANY_NAME
    """
    
    # Execute the query and load the data into a DataFrame
    df = pd.read_sql(query, engine)
    df = df.sort_values(by='kitchen_name')
    
    # Remove exact duplicates
    df = df.drop_duplicates(subset=['kitchen_name', 'company_name', 'LICENSE_START_DATE', 'LICENSE_EXPIRE_DATE',"Mail_To", "Mail_Cc"])
    
    #rename licenses_df columns from COMPANY_NAME to company_name and KICHEN_NAME to kitchen_name
    licenses_df.rename(columns={'COMPANY_NAME':'company_name', 'KICHEN_NAME':'kitchen_name'}, inplace=True)

    # Join the main query result with the licenses_df to filter out expired licenses
    df = df.merge(licenses_df, on=['kitchen_name', 'company_name'], suffixes=('', '_y'), how='inner')

    # Keep only the necessary columns and drop duplicates
    df = df[['kitchen_name', 'company_name', 'LICENSE_START_DATE', 'LICENSE_EXPIRE_DATE',"Mail_To", "Mail_Cc"]].drop_duplicates()
    
    # Close the database connection
    engine.dispose()
    
    return df  # Full DF of company names

def get_companies(Dummies=True, Expired = False):
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
            for restaurant, company in old_kitchens + trial_kitchens + demo_kitchens:
                if company:
                    conditions.append(f"(ks.KICHEN_NAME = '{restaurant}' AND cp.COMPANY_NAME = '{company}')")
                else:
                    conditions.append(f"ks.KICHEN_NAME = '{restaurant}'")
            
            if Expired:
                conditions = [cond for cond in conditions if "trial" in cond or "demo" in cond]
        
        elif Expired:
            for restaurant, company in old_kitchens:
                if company:
                    conditions.append(f"(ks.KICHEN_NAME = '{restaurant}' AND cp.COMPANY_NAME = '{company}')")
                else:
                    conditions.append(f"ks.KICHEN_NAME = '{restaurant}'")
        
        if conditions:
            conditions_str = " AND NOT (" + " OR ".join(conditions) + ")"
            query_conditions += conditions_str
        
        if Dummies and not Expired:
            current_date = datetime.now()
            licenses_df = licenses_df[licenses_df['LICENSE_EXPIRE_DATE'] >= current_date]
    
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
    df = df[~df[['kitchen_name', 'company_name']].apply(tuple, axis=1).isin(excluded_kitchens_set)]
    
    # Filter out inputs that are 0g
    df = df[df['input'] > 0]
    
    conversion_factors = {
        'KILOGRAM': 1000,   # kilograms to grams
        'POUND': 453.592, # pounds to grams
        'GRAM': 1         # grams to grams
    }

    # Convert input amounts to grams
    df['input_in_grams'] = df.apply(lambda row: row['input'] * conversion_factors.get(row['weight_unit'], 1), axis=1)
    
    return df


# Main Calculation (ADDED =None so we can input nothing and not None x times, 2024-07-03)
def get_food_waste_and_covers(start_date='2000-01-01', end_date=datetime.now(), company_name=None, restaurant_name=None, shift=None, category=None, food_type=None, CONS=False, SHIFT_ID=True, IGD_FOODTYPE_ID=True, IGD_CATEGORY_ID=True, OPERATION_DATE=True, Dummies=True, Expired=False):
    engine = create_connection()
    
    if not company_name and not restaurant_name:
        kitchen_df = get_kitchen(Dummies=Dummies, Expired=Expired)
        company_name = kitchen_df['company_name'].unique().tolist()
        restaurant_name = kitchen_df['kitchen_name'].unique().tolist()
    
    A = ""
    if CONS:
        A += "kfw.COMPLETE='Y' AND "
    if company_name:
        A += f"cp.COMPANY_NAME = '{escape_sql_string(company_name)}' AND "
    if restaurant_name:
        A += f"ks.KICHEN_NAME = '{escape_sql_string(restaurant_name)}' AND "
    if shift and shift != 'ALL':
        A += f'kfw.SHIFT_ID="{shift.upper()}" AND '
    if category and category != 'ALL':
        A += f'kfw.IGD_CATEGORY_ID="{category.upper()}" AND '
    if food_type and food_type != 'ALL':
        A += f'kfw.IGD_FOODTYPE_ID="{food_type.upper()}" AND '

    if IGD_FOODTYPE_ID or IGD_CATEGORY_ID:
        CV_AMOUNT = "kc.AMOUNT"
    elif not SHIFT_ID or not OPERATION_DATE:
        CV_AMOUNT = "SUM(kc.AMOUNT)"
    else:
        CV_AMOUNT = "kc.AMOUNT"
    
    fw_cv_q = f"""
    SELECT 
        DATE(kfw.OPERATION_DATE) as OPERATION_DATE,
        cp.COMPANY_NAME,
        ks.KICHEN_NAME,
        kfw.SHIFT_ID,
        kfw.IGD_CATEGORY_ID,
        kfw.IGD_FOODTYPE_ID,
        SUM(kfw.AMOUNT) as FW,
        {CV_AMOUNT} as CV
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
        kfw.ACTIVE = 'Y' AND
        {A}
        kfw.OPERATION_DATE BETWEEN '{start_date}' AND '{end_date}'
    GROUP BY 
        cp.COMPANY_NAME, ks.KICHEN_NAME
    """
    
    if OPERATION_DATE:
        fw_cv_q += ", kfw.OPERATION_DATE"
    if SHIFT_ID:
        fw_cv_q += ", kfw.SHIFT_ID"
    if IGD_CATEGORY_ID:
        fw_cv_q += ", kfw.IGD_CATEGORY_ID"
    if IGD_FOODTYPE_ID:
        fw_cv_q += ", kfw.IGD_FOODTYPE_ID"
        
    fwcv_comp = pd.read_sql_query(fw_cv_q, engine)
    fwcv_comp['OPERATION_DATE'] = pd.to_datetime(fwcv_comp['OPERATION_DATE'])
    return fwcv_comp

# Get full covers for all restaurants for each shift
def get_covers(start_date='2000-01-01', end_date=datetime.now(), company_name=None, restaurant_name=None, shift='ALL', category='ALL', food_type='ALL', CONS=False, SHIFT_ID=True, OPERATION_DATE=True, Dummies=True, Expired = False):
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
            for restaurant, company in old_kitchens + trial_kitchens + demo_kitchens:
                if company:
                    conditions.append(f"(ks.KICHEN_NAME = '{restaurant}' AND cp.COMPANY_NAME = '{company}')")
                else:
                    conditions.append(f"ks.KICHEN_NAME = '{restaurant}'")
        
            if Expired:
                conditions = [cond for cond in conditions if "trial" in cond or "demo" in cond]
        
        elif Expired:
            for restaurant, company in old_kitchens:
                if company:
                    conditions.append(f"(ks.KICHEN_NAME = '{restaurant}' AND cp.COMPANY_NAME = '{company}')")
                else:
                    conditions.append(f"ks.KICHEN_NAME = '{restaurant}'")
        
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
            for restaurant, company in old_kitchens + trial_kitchens + demo_kitchens:
                if company:
                    condition_list.append(f"(ks.KICHEN_NAME = '{restaurant}' AND cp.COMPANY_NAME = '{company}')")
                else:
                    condition_list.append(f"ks.KICHEN_NAME = '{restaurant}'")
        
            if Expired:
                condition_list = [cond for cond in condition_list if "trial" in cond or "demo" in cond]
        
        elif Expired:
            for restaurant, company in old_kitchens:
                if company:
                    condition_list.append(f"(ks.KICHEN_NAME = '{restaurant}' AND cp.COMPANY_NAME = '{company}')")
                else:
                    condition_list.append(f"ks.KICHEN_NAME = '{restaurant}'")
        
        if condition_list:
            condition_str = " AND NOT (" + " OR ".join(condition_list) + ")"
            query_conditions += condition_str
        
        if Dummies and not Expired:
            current_date = datetime.now()
            licenses_df = licenses_df[licenses_df['LICENSE_EXPIRE_DATE'] >= current_date]

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
                query+= f"(cp.COMPANY_NAME LIKE '%{company_name}%') OR "
                
        else:      
            query += f" AND cp.COMPANY_NAME LIKE '%{company_name}%'"
        
    if restaurant_name:
        query += f" AND ks.KICHEN_NAME LIKE '%{restaurant_name}%'"
        
    query += " ORDER BY ks.KICHEN_NAME, kb.BASELINE_END_DATE DESC"
    
    # Execute the query and load the data into a DataFrame
    df = pd.read_sql(query, engine)

    # Remove exact duplicates (rows with the same restaurant_name, company_name, start_date, and end_date)
    df = df.drop_duplicates(subset=['restaurant_name', 'company_name', 'start_date', 'end_date'])

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
            for restaurant, company in old_kitchens + trial_kitchens + demo_kitchens:
                if company:
                    conditions.append(f"(ks.KICHEN_NAME = '{restaurant}' AND cp.COMPANY_NAME = '{company}')")
                else:
                    conditions.append(f"ks.KICHEN_NAME = '{restaurant}'")
        
            if Expired:
                conditions = [cond for cond in conditions if "trial" in cond or "demo" in cond]
        
        elif Expired:
            for restaurant, company in old_kitchens:
                if company:
                    conditions.append(f"(ks.KICHEN_NAME = '{restaurant}' AND cp.COMPANY_NAME = '{company}')")
                else:
                    conditions.append(f"ks.KICHEN_NAME = '{restaurant}'")
        
        if conditions:
            conditions_str = " AND NOT (" + " OR ".join(conditions) + ")"
            query_conditions += conditions_str
        
        if Dummies and not Expired:
            current_date = datetime.now()
            licenses_df = licenses_df[licenses_df['LICENSE_EXPIRE_DATE'] >= current_date]
    
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
    df['post_baseline_start_date'] = df['baseline_end_date'] + pd.Timedelta(days=1)
    df['post_baseline_end_date'] = current_date
    
    return df[['restaurant_name', 'company_name', 'post_baseline_start_date', 'post_baseline_end_date']]

# Function to calculate grams per cover for a specific type (updated)
def g_cover(start_date='2000-01-01', end_date=datetime.now(), company_name=None, restaurant_name=None, shift=None, category=None, food_type=None, CONS=False, SHIFT_ID=True, IGD_FOODTYPE_ID=True, IGD_CATEGORY_ID=True, OPERATION_DATE=True, Dummies=True, Expired=False, grouping='overall'):
    
    fwcv_comp = get_food_waste_and_covers(start_date = start_date, end_date = end_date, company_name = company_name, restaurant_name = restaurant_name, shift = shift, category = category, food_type =food_type, CONS = CONS,IGD_CATEGORY_ID= IGD_CATEGORY_ID, IGD_FOODTYPE_ID=IGD_FOODTYPE_ID,SHIFT_ID=SHIFT_ID, Dummies=Dummies, Expired=Expired, OPERATION_DATE=OPERATION_DATE)

    if fwcv_comp.empty:
        return "No data available for the given parameters."

    # Group by the desired time period
    #BETTER AGGREGATION, AND MOST IMPORTANTLY DO THE CALCULATION AFTER AGG
    fwcv_comp['OPERATION_DATE']=pd.to_datetime(fwcv_comp['OPERATION_DATE'])
    fwcv_comp=fwcv_comp[['COMPANY_NAME','KICHEN_NAME','OPERATION_DATE','FW','CV']]
    if grouping == 'weekly':
        fwcv_comp = fwcv_comp.groupby([pd.Grouper(key='OPERATION_DATE', freq='W'),'KICHEN_NAME','COMPANY_NAME']).sum()

    elif grouping == 'monthly':
        fwcv_comp = fwcv_comp.groupby([pd.Grouper(key='OPERATION_DATE', freq='M'),'KICHEN_NAME','COMPANY_NAME']).sum()

    elif grouping == 'yearly':
        fwcv_comp = fwcv_comp.groupby([pd.Grouper(key='OPERATION_DATE', freq='Y'),'KICHEN_NAME','COMPANY_NAME']).sum()
    elif grouping == 'overall':
        fwcv_comp = fwcv_comp[['COMPANY_NAME','KICHEN_NAME','FW','CV']].groupby(['KICHEN_NAME','COMPANY_NAME']).sum()
    elif grouping == 'daily':
        pass
    else:
        raise ValueError("Invalid grouping specified. Use 'daily', 'weekly', 'monthly', or 'yearly'.")

    fwcv_comp['g_per_cover'] = (fwcv_comp['FW'] / fwcv_comp['CV']) * 1000

    fwcv_comp.rename(columns={'FW': 'Total_Food_Waste', 'CV': 'Total_Covers'}, inplace=True)

    return fwcv_comp

# DCON (Ilann's Formula)
def DCON(start_date='2000-01-01', end_date=datetime.now(), company_name=None, restaurant_name=None, TakingBaseline=True, grouping='overall', PerHotel=False, CONS=True, Dummies=True, Expired=False):
    engine = create_connection()
    conditions = ''
    # Construct the query conditions
    if company_name:
        if isinstance(company_name, list):
            conditions += "AND ("
            for comp in company_name:
                conditions+= f"""(cp.COMPANY_NAME = "{comp}") OR """
            conditions = conditions[:-3]
            conditions += ")"
        else:    
            conditions += f""" AND cp.COMPANY_NAME LIKE "%{company_name}%" """
    if restaurant_name:
        conditions += f"""AND ks.KICHEN_NAME LIKE "%{restaurant_name}%" """
    

    email_query = ""
    
    
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

    if Dummies or Expired:
        condition_list = []
        if Dummies:
            for restaurant, company in old_kitchens + trial_kitchens + demo_kitchens:
                if company:
                    condition_list.append(f"(ks.KICHEN_NAME = '{restaurant}' AND cp.COMPANY_NAME = '{company}')")
                else:
                    condition_list.append(f"ks.KICHEN_NAME = '{restaurant}'")
        
            if Expired:
                condition_list = [cond for cond in condition_list if "trial" in cond or "demo" in cond]
        
        elif Expired:
            for restaurant, company in old_kitchens:
                if company:
                    condition_list.append(f"(ks.KICHEN_NAME = '{restaurant}' AND cp.COMPANY_NAME = '{company}')")
                else:
                    condition_list.append(f"ks.KICHEN_NAME = '{restaurant}'")
        
        if condition_list:
            condition_str = " AND NOT (" + " OR ".join(condition_list) + ")"
            conditions += condition_str
        
        if Dummies and not Expired:
            current_date = datetime.now()
            licenses_df = licenses_df[licenses_df['LICENSE_EXPIRE_DATE'] >= current_date]

    # Define the consistency check
    if CONS:
        join_cover = ""
        consistency_str = "kfw.COMPLETE='Y' AND "
    else:
        consistency_str = """kc.ACTIVE='Y' AND kfw.ACTIVE = 'Y' AND 
            (NOT(kc.AMOUNT IS NULL) OR ks.PRODUCTION_KITCHEN_FLAG='Y') AND
            cp.COMPANY_STATUS  = 'ACTIVE' AND
            cp.ACTIVE='Y' AND
            ks.KICHEN_STATUS = 'Y' AND
            ks.ACTIVE = 'Y' AND
            (kc.ACTIVE='Y' or kc.ACTIVE IS NULL) AND
            (cs.ACTIVE IS NULL or cs.ACTIVE='N') AND ((kfw.SHIFT_ID='BREAKFAST' AND kos.BREAKFAST='Y') OR 
             (kfw.SHIFT_ID='LUNCH' AND kos.LUNCH='Y') OR 
             (kfw.SHIFT_ID='DINNER' AND kos.DINNER='Y') OR 
             (kfw.SHIFT_ID='BRUNCH' AND kos.BRUNCH='Y') OR 
             (kfw.SHIFT_ID='AFTERNOON_TEA' AND kos.AFTERNOON_TEA='Y')) AND """
        
        join_cover = """LEFT JOIN KITCHEN_COVER kc ON kc.OPERATION_DATE=kfw.OPERATION_DATE AND kc.SHIFT_ID=kfw.SHIFT_ID AND kc.KC_STT_ID=ks.KC_STT_ID
        LEFT JOIN 
        KITCHEN_SHIFT_CLOSE cs ON cs.KC_STT_ID = ks.KC_STT_ID AND cs.SHIFT_ID = kfw.SHIFT_ID AND cs.CLOSE_DATE = kfw.OPERATION_DATE 
        JOIN 
            lightblue.KITCHEN_OPERATION_SHIFT kos ON kos.KC_STT_ID = ks.KC_STT_ID AND kos.DAY_OF_WEEK=UPPER(DAYNAME(kfw.OPERATION_DATE)) AND kos.OPERATION_SHIFT_TYPE='SHIFT_MAIN'
        
         """
    
    # Queries
    firstdate_query = f"""
        SELECT  
            cp.COMPANY_NAME, -- account
            ks.KICHEN_NAME, -- kitchen/outlet
            MIN(kb.BASELINE_END_DATE) as FirstDate,
            COUNTRY_CODE
        FROM 
            COMPANY_PROFILE cp
        JOIN 
            KITCHEN_STATION ks ON cp.CPN_PF_ID = ks.CPN_PF_ID
        JOIN 
            KITCHEN_BASELINE kb ON ks.KC_STT_ID = kb.KC_STT_ID
        LEFT JOIN
            lightblue.COMPANY_ACTIVATE ca ON ks.CPN_PF_ID = ca.CPN_PF_ID
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
    
    closed_shifts_query = f"""
        SELECT
            cp.COMPANY_NAME, -- company
            ks.KICHEN_NAME, -- kitchen
            ksc.CLOSE_DATE as OPERATION_DATE, -- which date
            ksc.SHIFT_ID -- which shift
        FROM 
            KITCHEN_SHIFT_CLOSE ksc 
        JOIN
            COMPANY_PROFILE cp on cp.CPN_PF_ID = ksc.CPN_PF_ID 
        LEFT JOIN
            lightblue.COMPANY_ACTIVATE ca ON cp.CPN_PF_ID = ca.CPN_PF_ID
        JOIN 
            KITCHEN_STATION ks on ks.KC_STT_ID = ksc.KC_STT_ID 
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
            COMPANY_PROFILE cp on kos.CPN_PF_ID = cp.CPN_PF_ID 
        JOIN 
            KITCHEN_STATION ks on kos.KC_STT_ID = ks.KC_STT_ID
        LEFT JOIN
            COMPANY_ACTIVATE ca ON ks.CPN_PF_ID = ca.CPN_PF_ID
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
    

    fw_cv_query = f"""
        SELECT 
            DATE(kfw.OPERATION_DATE) as OPERATION_DATE,
            cp.COMPANY_NAME,
            ks.KICHEN_NAME,
            kfw.SHIFT_ID
        FROM 
            lightblue.KITCHEN_FOOD_WASTE kfw 
        JOIN 
            lightblue.KITCHEN_STATION ks ON kfw.KC_STT_ID = ks.KC_STT_ID 
        LEFT JOIN
            lightblue.COMPANY_ACTIVATE ca ON ks.CPN_PF_ID = ca.CPN_PF_ID
        JOIN
            lightblue.COMPANY_PROFILE cp ON ks.CPN_PF_ID = cp.CPN_PF_ID
        {join_cover}
        WHERE 
            kfw.ACTIVE = 'Y' 
            {conditions} AND
            {consistency_str}
            kfw.OPERATION_DATE BETWEEN '{start_date}' AND '{end_date}'

        GROUP BY 
            cp.COMPANY_NAME, ks.KICHEN_NAME, kfw.OPERATION_DATE, kfw.SHIFT_ID
    """

    # Load data from SQL queries
    opening_shifts = pd.read_sql_query(opening_shift_query, engine).drop_duplicates()
    fwcv_comp = pd.read_sql_query(fw_cv_query, engine).drop_duplicates()
    closed_shifts = pd.read_sql_query(closed_shifts_query, engine).drop_duplicates()

    # Determine grouping for data 
    # changed M to ME, Y to YE to get rid of the deprecation warning
    if grouping == 'monthly':
        group = [pd.Grouper(key='OPERATION_DATE', freq='ME'), 'KICHEN_NAME', 'COMPANY_NAME']
    elif grouping == 'weekly':
        group = [pd.Grouper(key='OPERATION_DATE', freq='W-SUN'), 'KICHEN_NAME', 'COMPANY_NAME']  # Grouping by week ending on Sunday
    elif grouping == 'yearly':
        group = [pd.Grouper(key='OPERATION_DATE', freq='YE'), 'KICHEN_NAME', 'COMPANY_NAME']
    elif grouping == 'daily':
        group = ['OPERATION_DATE', 'KICHEN_NAME', 'COMPANY_NAME']
    else:
        group = ['KICHEN_NAME', 'COMPANY_NAME']

    # Merge baseline data if necessary
    if TakingBaseline:
        firstdate = pd.read_sql_query(firstdate_query, engine)
        fwcv_comp = fwcv_comp.merge(firstdate, on=['COMPANY_NAME', 'KICHEN_NAME'], how='left')
        fwcv_comp = fwcv_comp[(pd.to_datetime(fwcv_comp['OPERATION_DATE']) > pd.to_datetime(fwcv_comp['FirstDate'])) & (pd.to_datetime(fwcv_comp['OPERATION_DATE']) >= pd.to_datetime(start_date))]
        fwcv_comp = fwcv_comp.drop(['FirstDate', 'COUNTRY_CODE'], axis=1)
        # getting rid of closed shifts before first date (during baseline)
        closed_shifts = closed_shifts.merge(firstdate, on=['COMPANY_NAME', 'KICHEN_NAME'], how='left')
        closed_shifts = closed_shifts[(pd.to_datetime(closed_shifts['OPERATION_DATE']) > pd.to_datetime(closed_shifts['FirstDate'])) & (pd.to_datetime(closed_shifts['OPERATION_DATE']) >= pd.to_datetime(start_date))]
        closed_shifts = closed_shifts.drop(['FirstDate', 'COUNTRY_CODE'], axis=1)

    # Function to get all dates for a given day of the week within the specified range
    def get_dates_for_day(day_of_week, start_date, end_date):
        all_dates = pd.date_range(start=start_date, end=end_date)
        return all_dates[all_dates.day_name() == day_of_week.title()]

    # Generate the full schedule of shifts
    full_schedule_data = []
    for index, row in opening_shifts.iterrows():
        dates = get_dates_for_day(row['DAY_OF_WEEK'], start_date, end_date)
        for date in dates:
            shift_ids = ['BREAKFAST', 'BRUNCH', 'LUNCH', 'AFTERNOON_TEA', 'DINNER']
            for shift in shift_ids:
                if row[shift] == 'Y':
                    # Check if the entry already exists to avoid duplicates
                    if not any((entry['COMPANY_NAME'] == row['COMPANY_NAME'] and
                                entry['KICHEN_NAME'] == row['KICHEN_NAME'] and
                                entry['OPERATION_DATE'] == date and
                                entry['SHIFT_ID'] == shift) for entry in full_schedule_data):
                        full_schedule_data.append({
                            'COMPANY_NAME': row['COMPANY_NAME'],
                            'KICHEN_NAME': row['KICHEN_NAME'],
                            'OPERATION_DATE': date,
                            'SHIFT_ID': shift,
                            'TOTAL_SHIFTS': 1
                        })

    full_schedule_df = pd.DataFrame(full_schedule_data)
    if TakingBaseline:
        full_schedule_df = full_schedule_df.merge(firstdate, on=['COMPANY_NAME', 'KICHEN_NAME'], how='left')
        full_schedule_df = full_schedule_df[full_schedule_df['OPERATION_DATE'] > full_schedule_df['FirstDate']]
    # Mark redundant closed shifts
    def is_redundant_shift(x):
        try:
            if opening_shifts.where((opening_shifts['DAY_OF_WEEK'] == x['OPERATION_DATE'].day_name().upper()) & 
                                    (opening_shifts['KICHEN_NAME'] == x['KICHEN_NAME']) & 
                                    (opening_shifts['COMPANY_NAME'] == x['COMPANY_NAME']))[x['SHIFT_ID']].dropna().item() == 'N':
                return False
        except:
            return True
        return True

    closed_shifts['IS_REDUNDANT'] = closed_shifts.apply(lambda x: 'N' if is_redundant_shift(x) else 'Y', axis=1)
    closed_shifts['OPERATION_DATE'] = pd.to_datetime(closed_shifts['OPERATION_DATE'])
    closed_shifts = closed_shifts[closed_shifts['IS_REDUNDANT'] == 'N']

    # Set initial values for comparison
    fwcv_comp['COMP_SHIFTS'] = 1
    closed_shifts['CLOSED_SHIFTS'] = 1
    fwcv_comp['OPERATION_DATE'] = pd.to_datetime(fwcv_comp['OPERATION_DATE'])
    full_schedule_df['OPERATION_DATE'] = pd.to_datetime(full_schedule_df['OPERATION_DATE'])
    fwcv_comp = fwcv_comp.drop_duplicates()
    # Group data based on the selected grouping
    fwcv_comp_grouped = fwcv_comp.groupby(group).size().reset_index(name='COMP_SHIFTS')
    closed_shifts_grouped = closed_shifts.groupby(group).size().reset_index(name='CLOSED_SHIFTS')
    full_schedule_grouped = full_schedule_df.groupby(group).size().reset_index(name='TOTAL_SHIFTS')
    if group != ['KICHEN_NAME', 'COMPANY_NAME']:
        group = ['OPERATION_DATE', 'KICHEN_NAME', 'COMPANY_NAME']
    
    open_shifts = pd.merge(left=full_schedule_grouped, right=fwcv_comp_grouped, on=group, how='left')
    dcon_data = open_shifts.merge(closed_shifts_grouped, on=group, how='left')

    dcon_data['COMP_SHIFTS'] = dcon_data['COMP_SHIFTS'].fillna(0)
    dcon_data['CLOSED_SHIFTS'] = dcon_data['CLOSED_SHIFTS'].fillna(0)
    dcon_data = dcon_data.reset_index()
    dcon_data = dcon_data.merge(licenses_df, on=["COMPANY_NAME", "KICHEN_NAME"], how="inner")

    # Final grouping and calculation of consistency
    group = []
    if grouping in ['daily', 'weekly', 'yearly', 'monthly']:
        group.append('OPERATION_DATE')
    group.append('COMPANY_NAME')
    if PerHotel:
        dcon_data = dcon_data.groupby(group).sum().reset_index()
    else:
        group.append('KICHEN_NAME')

    dcon_data['CONSISTENCY'] = (dcon_data['COMP_SHIFTS'] / (dcon_data['TOTAL_SHIFTS'] - dcon_data['CLOSED_SHIFTS'])).round(2)
    dcon_data['CONSISTENCY'] = dcon_data['CONSISTENCY'].replace([np.inf, -np.inf], 0)
    dcon_data['CONSISTENCY'] = dcon_data['CONSISTENCY'].fillna(0)

    group += ['CONSISTENCY', 'TOTAL_SHIFTS', 'CLOSED_SHIFTS', 'COMP_SHIFTS']
    
    if PerHotel:
        dcon_data = dcon_data[group].sort_values(by=['COMPANY_NAME'])
    else:
        dcon_data = dcon_data[group].sort_values(by=['COMPANY_NAME', 'KICHEN_NAME'])

    return dcon_data

# Savings 
def get_savings(start_date=None, end_date=None, CONS=False, company_name=None, restaurant_name=None, Baseline_Entry=None, shift=None, category=None, foodtype=None, Dummies=True, with_old_calc=False, MergeKitchen=False, MergeComp=False, Expired=False):
    load_dotenv()

    old_new_condition = "kfw.COMPLETE='Y' AND "
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
             (kfw.SHIFT_ID='AFTERNOON_TEA' AND kos.AFTERNOON_TEA='Y')))) AND """

    start_date = start_date or '2000-01-01'
    end_date = end_date or datetime.now()
    if Baseline_Entry:
        Baseline_Entry = (pd.to_datetime(Baseline_Entry[0]), pd.to_datetime(Baseline_Entry[1]))

    engine = create_connection()
    kitchen_column = '' if MergeKitchen else ', ks.KICHEN_NAME'

    query_conditions = []
    Dummies_String=""

    if Dummies:
            conditions=[]
            for restaurant, company in old_kitchens + trial_kitchens + demo_kitchens:
                if company:
                    conditions.append(f"(ks.KICHEN_NAME = '{restaurant}' AND cp.COMPANY_NAME = '{company}')")
                else:
                    conditions.append(f"ks.KICHEN_NAME = '{restaurant}'")
            Dummies_String= " NOT "+" OR ".join(conditions)+" AND "
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
        

    conditions_str = ' AND '.join(query_conditions) + ' AND ' if query_conditions else ''
    kitchen_join = "LEFT JOIN lightblue.COMPANY_ACTIVATE ca ON ks.CPN_PF_ID = ca.CPN_PF_ID" if Expired else ''
    CONS_For_Baseline = ""
    if CONS:
        CONS_For_Baseline = "kfw.COMPLETE='Y' AND "
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
        kfw.ACTIVE = 'Y' AND
        {conditions_str}{old_new_condition}{Dummies_String}
        kc.ACTIVE='Y' AND
        kfw.OPERATION_DATE BETWEEN '{start_date}' AND '{end_date}' 
    GROUP BY 
    cp.COMPANY_NAME{kitchen_column}, kfw.OPERATION_DATE, kfw.SHIFT_ID"""

    fw_cv_b_query=f"""
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
        kfw.ACTIVE = 'Y' AND
        {conditions_str}{old_new_condition}{CONS_For_Baseline}
        kc.ACTIVE='Y'
    GROUP BY 
    cp.COMPANY_NAME{kitchen_column}, kfw.OPERATION_DATE, kfw.SHIFT_ID"""


    fw_cv_comp = pd.read_sql_query(fw_cv_query, engine)
    if CONS or start_date or end_date:
        fw_cv_comp_baseline = pd.read_sql_query(fw_cv_b_query, engine)
    else:
        fw_cv_comp_baseline = fw_cv_comp

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

    fw_cv_comp_baseline['OPERATION_DATE'] = pd.to_datetime(fw_cv_comp_baseline['OPERATION_DATE'])
    fw_cv_comp['OPERATION_DATE'] = pd.to_datetime(fw_cv_comp['OPERATION_DATE'])

    def calculate_fwcv(baseline_row, shift):
        condition = (fw_cv_comp_baseline['COMPANY_NAME'] == baseline_row['company_name']) & \
                    (fw_cv_comp_baseline['KICHEN_NAME'] == baseline_row['restaurant_name']) & \
                    (baseline_row['end_date'] >= fw_cv_comp_baseline['OPERATION_DATE']) & \
                    (baseline_row['start_date'] <= fw_cv_comp_baseline['OPERATION_DATE'])
        if shift:
            condition &= (fw_cv_comp_baseline['SHIFT_ID'] == shift)
        
        relevant_data = fw_cv_comp_baseline[condition][['CV', 'FW']].sum()

        if relevant_data['CV'] == 0:
            return 'Na'
        return relevant_data['FW'] / relevant_data['CV']

    baseline_data['start_date'] = pd.to_datetime(baseline_data['start_date'])
    baseline_data['end_date'] = pd.to_datetime(baseline_data['end_date'])
    baseline_data['FWCV'] = baseline_data.apply(lambda row: calculate_fwcv(row, ''), axis=1)
    baseline_data['FWCV_BREAKFAST'] = baseline_data.apply(lambda row: calculate_fwcv(row, 'BREAKFAST'), axis=1)
    baseline_data['FWCV_DINNER'] = baseline_data.apply(lambda row: calculate_fwcv(row, 'DINNER'), axis=1)
    baseline_data['FWCV_LUNCH'] = baseline_data.apply(lambda row: calculate_fwcv(row, 'LUNCH'), axis=1)
    baseline_data['FWCV_BRUNCH'] = baseline_data.apply(lambda row: calculate_fwcv(row, 'BRUNCH'), axis=1)
    baseline_data['FWCV_AFTERNOON_TEA'] = baseline_data.apply(lambda row: calculate_fwcv(row, 'AFTERNOON_TEA'), axis=1)
    baseline_data = baseline_data[baseline_data['FWCV'] != 'Na']

    def calculate_savings(row, start_date, end_date, use_first_baseline):
        st_date = start_date

        try:
            relevant_baselines = baseline_data[(baseline_data['company_name'] == row['COMPANY_NAME']) & 
                                               (baseline_data['restaurant_name'] == row['KICHEN_NAME'])].sort_values('end_date', ascending=True).reset_index()
            if relevant_baselines.empty:
                return None
        except Exception as e:
            print(e)
            return None

        current_baseline_index = 0
        list_end_dates = ['2000-01-01']

        for idx in range(len(relevant_baselines) + 1):
            if idx != len(relevant_baselines):
                list_end_dates.append(relevant_baselines['end_date'][idx])
            else:
                list_end_dates.append('2100-01-01')

            if pd.to_datetime(list_end_dates[idx + 1]) > pd.to_datetime(st_date) > pd.to_datetime(list_end_dates[idx]):
                current_baseline_index = idx - 1

        if current_baseline_index == -1 and not relevant_baselines.empty:
            st_date = relevant_baselines['end_date'][0] + timedelta(days=1)
            current_baseline_index = 0
            current_baseline = relevant_baselines.iloc[0]
        elif current_baseline_index == len(relevant_baselines) - 1:
            current_baseline = relevant_baselines.iloc[len(relevant_baselines) - 1]
        else:
            current_baseline = relevant_baselines.iloc[current_baseline_index]

        if use_first_baseline:
            current_baseline = relevant_baselines.iloc[0]
            current_baseline_index = 0

        if Baseline_Entry:
            for index, bl_entry in relevant_baselines.iterrows():
                if pd.to_datetime(bl_entry['end_date']) == Baseline_Entry[1] and pd.to_datetime(bl_entry['start_date']) == Baseline_Entry[0]:
                    current_baseline = bl_entry

        if pd.to_datetime(current_baseline['end_date']) > pd.to_datetime(st_date):
            st_date = pd.to_datetime(current_baseline['end_date']) + timedelta(days=1)

        data_within_dates = fw_cv_comp[(fw_cv_comp['COMPANY_NAME'] == row['COMPANY_NAME']) &
                                       (fw_cv_comp['KICHEN_NAME'] == row['KICHEN_NAME']) &
                                       (pd.to_datetime(end_date) >= pd.to_datetime(fw_cv_comp['OPERATION_DATE'])) &
                                       (pd.to_datetime(st_date) <= pd.to_datetime(fw_cv_comp['OPERATION_DATE']))][['CV', 'FW']].sum()

        sorted_data = fw_cv_comp[(fw_cv_comp['COMPANY_NAME'] == row['COMPANY_NAME']) &
                                 (fw_cv_comp['KICHEN_NAME'] == row['KICHEN_NAME'])].sort_values('OPERATION_DATE', ascending=True).reset_index()

        grouped_data = fw_cv_comp[(fw_cv_comp['COMPANY_NAME'] == row['COMPANY_NAME']) &
                                  (fw_cv_comp['KICHEN_NAME'] == row['KICHEN_NAME']) &
                                  (pd.to_datetime(end_date) >= pd.to_datetime(fw_cv_comp['OPERATION_DATE'])) &
                                  (pd.to_datetime(st_date) <= pd.to_datetime(fw_cv_comp['OPERATION_DATE']))].groupby('OPERATION_DATE').sum()

        sorted_data['FWCV'] = sorted_data['FW'] / sorted_data['CV']
        food_saved_initial = 0
        food_saved_multiple = 0
        current_baseline_copy = current_baseline.copy()

        for idx in range(len(sorted_data)):
            if current_baseline['FWCV_' + sorted_data['SHIFT_ID'][idx]] != 'Na' and sorted_data['FWCV'][idx] < current_baseline['FWCV_' + sorted_data['SHIFT_ID'][idx]] and sorted_data['CV'][idx] != 0:
                food_saved_initial += (-sorted_data['FWCV'][idx] + current_baseline['FWCV_' + sorted_data['SHIFT_ID'][idx]]) * sorted_data['CV'][idx]

        if len(relevant_baselines) != 1:
            for idx in range(len(sorted_data)):
                while current_baseline_index != len(relevant_baselines) - 1 and sorted_data['OPERATION_DATE'][idx] > relevant_baselines['end_date'][current_baseline_index + 1]:
                    current_baseline_copy = relevant_baselines.loc[current_baseline_index + 1]
                    current_baseline_index += 1

                if current_baseline_copy['FWCV_' + sorted_data['SHIFT_ID'][idx]] != 'Na' and sorted_data['FWCV'][idx] < current_baseline_copy['FWCV_' + sorted_data['SHIFT_ID'][idx]] and sorted_data['CV'][idx] != 0:
                    food_saved_multiple += (-sorted_data['FWCV'][idx] + current_baseline_copy['FWCV_' + sorted_data['SHIFT_ID'][idx]]) * sorted_data['CV'][idx]
        else:
            food_saved_multiple = food_saved_initial

        if data_within_dates['CV'] == 0 or current_baseline['FWCV'] == 'Na' or current_baseline['FWCV'] == 0:
            return None
        else:
            return [data_within_dates['FW'] / data_within_dates['CV'], -1 + ((data_within_dates['FW'] / data_within_dates['CV']) / current_baseline['FWCV']), food_saved_initial, food_saved_multiple, current_baseline['FWCV'],
                    current_baseline['start_date'], current_baseline['end_date'], st_date, end_date, current_baseline['COUNTRY_CODE'],
                    grouped_data['FW'].sum() / grouped_data['FW'].count(), grouped_data['FW'].sum(), grouped_data['CV'].sum()]

    results = fw_cv_comp[['COMPANY_NAME', 'KICHEN_NAME']].drop_duplicates().apply(lambda x: calculate_savings(x, start_date, end_date, True), axis=1).to_frame().join(fw_cv_comp[['COMPANY_NAME', 'KICHEN_NAME']].drop_duplicates()).dropna()

    initial_fwcv = []
    post_baseline_fwcv = []
    fwcv_variation = []
    food_saved_initial = []
    food_saved_multiple = []
    baseline_start_dates = []
    baseline_end_dates = []
    post_baseline_start_dates = []
    post_baseline_end_dates = []
    country_codes = []
    daily_food_waste_avg = []
    total_food_waste = []
    total_covers = []
    result_array = np.array(results)

    for entry in result_array:
        initial_fwcv.append(entry[0][4] * 1000)
        post_baseline_fwcv.append(entry[0][0] * 1000)
        fwcv_variation.append(round(entry[0][1], 3) * 100)
        if np.round(entry[0][2], 5) != np.round(entry[0][3], 5):
            food_saved_initial.append(entry[0][2])
            food_saved_multiple.append(entry[0][3])
        else:
            food_saved_initial.append(entry[0][2])
            food_saved_multiple.append(entry[0][2])

        baseline_start_dates.append(entry[0][5])
        post_baseline_start_dates.append(entry[0][7])
        baseline_end_dates.append(entry[0][6])
        post_baseline_end_dates.append(entry[0][8])
        country_codes.append(entry[0][9])
        daily_food_waste_avg.append(entry[0][10])
        total_food_waste.append(entry[0][11])
        total_covers.append(entry[0][12])

    results['COUNTRY CODE'] = country_codes
    results['g/cover during Baseline 1'] = initial_fwcv
    results['BL 1 Start Date'] = baseline_start_dates
    results['BL 1 End Date'] = baseline_end_dates
    results['g/cover PBL'] = post_baseline_fwcv
    results['PBL Start Date'] = post_baseline_start_dates
    results['PBL End Date'] = post_baseline_end_dates
    results['FWCV variation'] = fwcv_variation
    results['saved food first Baseline (in kg)'] = food_saved_initial
    results['saved food Multiple Baseline (in kg)'] = food_saved_multiple
    results['daily kg wasted'] = daily_food_waste_avg
    results['kg wasted'] = total_food_waste
    results['Number of Covers'] = total_covers

    return results.drop(0, axis=1)
