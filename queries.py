import pandas as pd
# Fetch data based on form input
def fetch_total_fw(engine, company_name, start_date, end_date):
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
    WHERE kfw.ACTIVE ='Y'
    AND
    cp.COMPANY_STATUS = 'ACTIVE' 
    AND
    ks.KICHEN_STATUS = 'Y' 
    AND
    ks.ACTIVE = 'Y'
    AND
    cp.COMPANY_NAME LIKE '%{company_name}%' and (kfw.OPERATION_DATE BETWEEN '{start_date}' AND '{end_date}')
    ORDER BY kfw.OPERATION_DATE;"""
    return pd.read_sql_query(query, engine)

def fetch_fw_entries(engine, company_name, start_date, end_date):
    # QUERY
    query = f"""SELECT 
    DATE(kfw.OPERATION_DATE) as OPERATION_DATE, -- what day
    cp.COMPANY_NAME, -- which property
    ks.KICHEN_NAME, -- kitchen
    kfw.SHIFT_ID, -- shift   
    kfw.IGD_CATEGORY_ID, -- cat
    kfw.AMOUNT, -- kg 
    kfw.IGD_FOODTYPE_ID -- type 
    FROM 
    KITCHEN_FOOD_WASTE kfw 
    JOIN 
    KITCHEN_STATION ks ON kfw.KC_STT_ID = ks.KC_STT_ID 
    JOIN 
    COMPANY_PROFILE cp ON ks.CPN_PF_ID = cp.CPN_PF_ID
    WHERE kfw.ACTIVE ='Y' 
    AND 
    cp.COMPANY_STATUS  = 'ACTIVE' 
    AND
    cp.ACTIVE = 'Y' 
    AND
    ks.KICHEN_STATUS = 'Y' 
    AND
    ks.ACTIVE = 'Y'
    AND cp.COMPANY_NAME LIKE '%{company_name}%' and (kfw.OPERATION_DATE BETWEEN '{start_date}' AND '{end_date}')
    ORDER BY ks.KICHEN_NAME, kfw.OPERATION_DATE;
    """
    return pd.read_sql_query(query, engine)

def fetch_cv_entries(engine, company_name, start_date, end_date):
    # QUERY
    query = f"""SELECT 
    DATE(kc.OPERATION_DATE) as OPERATION_DATE,    
    cp.COMPANY_NAME, -- which property
    ks.KICHEN_NAME, -- kitchen
    kc.SHIFT_ID,
    kc.AMOUNT
    FROM 
    KITCHEN_COVER kc  
    JOIN 
    KITCHEN_STATION ks ON kc.KC_STT_ID = ks.KC_STT_ID 
    JOIN 
    COMPANY_PROFILE cp ON ks.CPN_PF_ID = cp.CPN_PF_ID
    WHERE kc.ACTIVE ='Y'
    AND
    cp.COMPANY_STATUS  = 'ACTIVE' 
    AND
    cp.ACTIVE = 'Y' 
    AND
    ks.KICHEN_STATUS = 'Y' 
    AND
    ks.ACTIVE = 'Y'
    AND cp.COMPANY_NAME LIKE '%{company_name}%' and (kc.OPERATION_DATE BETWEEN '{start_date}' AND '{end_date}')
    ORDER BY ks.KICHEN_NAME, kc.OPERATION_DATE;""" 
    return pd.read_sql_query(query, engine)