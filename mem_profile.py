# profile_dcon.py
from calculations import DCON, create_connection
from sqlalchemy import create_engine
from memory_profiler import profile

engine = create_connection()

# Profile the DCON function
@profile
def profile_dcon():
    # Replace these with test parameters
    start_date = '2024-10-01'
    end_date = '2024-10-20'
    grouping = 'weekly'
    CONS = False

    # Call the DCON function and profile it
    result = DCON(start_date, end_date, grouping, CONS)
    return result

if __name__ == '__main__':
    profile_dcon()
