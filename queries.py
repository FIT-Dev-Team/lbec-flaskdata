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

def fetch_blpr(engine):
    query = f"""SELECT 
    cp.COMPANY_NAME,
    ks.KICHEN_NAME,
    kb.BASELINE_START_DATE,
    kb.BASELINE_END_DATE
    FROM
    COMPANY_PROFILE cp 
    JOIN
    KITCHEN_STATION ks ON cp.CPN_PF_ID = ks.CPN_PF_ID 
    JOIN 
    KITCHEN_BASELINE kb ON kb.KC_STT_ID = ks.KC_STT_ID 
    WHERE cp.COMPANY_STATUS = 'ACTIVE'
    AND 
    cp.ACTIVE = 'Y'
    AND 
    ks.KICHEN_STATUS = 'Y'
    AND 
    ks.ACTIVE = 'Y'
    ORDER BY cp.COMPANY_NAME, ks.KICHEN_NAME;"""
    return pd.read_sql_query(query, engine)

def fetch_first_date(engine, company_name):
    query = f"""
        SELECT  
       cp.COMPANY_NAME, -- account
       ks.KICHEN_NAME, -- kitchen/outlet
       MIN(kb.BASELINE_END_DATE) as FirstDate,
       COUNTRY_CODE
        FROM COMPANY_PROFILE cp
        JOIN KITCHEN_STATION ks ON cp.CPN_PF_ID = ks.CPN_PF_ID
        JOIN KITCHEN_BASELINE kb ON ks.KC_STT_ID = kb.KC_STT_ID
        WHERE kb.ACTIVE = 'Y' 
        AND cp.COMPANY_NAME LIKE '%{company_name}%'
        AND cp.COMPANY_STATUS = 'ACTIVE'
        AND cp.ACTIVE = 'Y' 
        AND ks.KICHEN_STATUS = 'Y'
        AND ks.ACTIVE = 'Y'
        GROUP BY cp.COMPANY_NAME, ks.KICHEN_NAME
        ORDER BY cp.COMPANY_NAME, ks.KICHEN_NAME;
        """
    return pd.read_sql_query(query, engine)

def fetch_fwcv(engine, company_name, end_date):
    query=f"""
    SELECT 
    DATE(kfw.OPERATION_DATE) as OPERATION_DATE,
    cp.COMPANY_NAME,
    ks.KICHEN_NAME,
    kfw.SHIFT_ID,
    kfw.IGD_CATEGORY_ID,
    SUM(kfw.AMOUNT) as FW,
    kc.AMOUNT as CV
    FROM 
    KITCHEN_FOOD_WASTE kfw 
    JOIN 
    KITCHEN_STATION ks ON kfw.KC_STT_ID = ks.KC_STT_ID 
    JOIN 
    COMPANY_PROFILE cp ON ks.CPN_PF_ID = cp.CPN_PF_ID
    LEFT JOIN 
    KITCHEN_COVER kc ON kc.KC_STT_ID = ks.KC_STT_ID AND kc.SHIFT_ID = kfw.SHIFT_ID AND kc.OPERATION_DATE = kfw.OPERATION_DATE
    LEFT JOIN 
    KITCHEN_SHIFT_CLOSE cs ON cs.KC_STT_ID = ks.KC_STT_ID AND cs.SHIFT_ID = kfw.SHIFT_ID AND cs.CLOSE_DATE = kfw.OPERATION_DATE 
    JOIN 
    KITCHEN_OPERATION_SHIFT kos ON kos.KC_STT_ID=ks.KC_STT_ID AND 
    kos.DAY_OF_WEEK=UPPER(DAYNAME(kfw.OPERATION_DATE)) AND
    kos.OPERATION_SHIFT_TYPE='SHIFT_MAIN'
    WHERE 
    kfw.ACTIVE = 'Y' AND 
    ( NOT(kc.AMOUNT IS NULL) OR ks.PRODUCTION_KITCHEN_FLAG='Y') AND
    cp.COMPANY_STATUS  = 'ACTIVE' AND
    cp.ACTIVE='Y' AND
    cp.COMPANY_NAME LIKE '%{company_name}%' AND
    ks.KICHEN_STATUS = 'Y' AND
    ks.ACTIVE = 'Y' AND
    (kc.ACTIVE='Y' or kc.ACTIVE IS NULL) AND
    (cs.ACTIVE IS NULL or cs.ACTIVE='N') AND
    kfw.OPERATION_DATE BETWEEN '2021-03-12' AND '{end_date}' 
    AND ((kfw.SHIFT_ID='BREAKFAST' AND kos.BREAKFAST='Y') 
    OR (kfw.SHIFT_ID='LUNCH' AND kos.LUNCH='Y') 
    OR (kfw.SHIFT_ID='DINNER' AND kos.DINNER='Y') OR (kfw.SHIFT_ID='BRUNCH' AND kos.BRUNCH='Y') 
    OR (kfw.SHIFT_ID='AFTERNOON_TEA' AND kos.AFTERNOON_TEA='Y'))
    GROUP BY 
    cp.COMPANY_NAME, ks.KICHEN_NAME, kfw.OPERATION_DATE, kfw.SHIFT_ID, kfw.IGD_CATEGORY_ID"""
    return pd.read_sql_query(query, engine)


# SQL query to get closed shifts information
def fetch_closed_shifts(engine, company_name, end_date):
    query = f"""
    SELECT
    cp.COMPANY_NAME, -- company
    ks.KICHEN_NAME, -- kitchen
    ksc.CLOSE_DATE, -- which date
    ksc.SHIFT_ID -- which shift
    FROM 
    KITCHEN_SHIFT_CLOSE ksc 
    JOIN
    COMPANY_PROFILE cp on cp.CPN_PF_ID = ksc.CPN_PF_ID 
    JOIN 
    KITCHEN_STATION ks on ks.KC_STT_ID = ksc.KC_STT_ID 
    WHERE ksc.ACTIVE = 'Y' 
    AND  cp.COMPANY_STATUS  = 'ACTIVE' 
    AND  ks.KICHEN_STATUS = 'Y' 
    AND ks.ACTIVE = 'Y'
    AND cp.COMPANY_NAME LIKE '%{company_name}%'
    AND ksc.CLOSE_DATE BETWEEN '2021-03-12' AND '{end_date}'
    ORDER BY cp.COMPANY_NAME , ks.KICHEN_NAME, ksc.CLOSE_DATE ;"""

    return pd.read_sql_query(query, engine)


# SQL query to get opening shifts information
def fetch_opening_shifts(engine, company_name):
    query = f"""
    Select
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
    WHERE kos.ACTIVE = 'Y'   
        AND cp.COMPANY_NAME LIKE '%{company_name}%' AND
        cp.COMPANY_STATUS = 'ACTIVE'
        AND 
        cp.ACTIVE = 'Y' 
        AND 
        ks.KICHEN_STATUS = 'Y'
        AND 
        ks.ACTIVE ='Y'
        and 
        kos.OPERATION_SHIFT_TYPE = 'SHIFT_MAIN'
    ORDER BY cp.COMPANY_NAME, ks.KICHEN_NAME;
    """
    return pd.read_sql_query(query, engine)
