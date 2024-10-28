#
from app import process_wdcon, create_connection
from sqlalchemy import create_engine
from memory_profiler import profile

engine = create_connection()

# Profile wdcon 
@profile
def profile_dcon():
    # Replace these with test parameters
    start_date = '2024-10-01'
    end_date = '2024-10-20'
    grouping = 'weekly'

    # Call the DCON function and profile it
    result = process_wdcon(start_date=start_date, end_date=end_date, grouping=grouping, CONS=CONS)
    return result

if __name__ == '__main__':
    profile_dcon()
