import os
import sys
import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import logging
from calculations import *
import plotly.express as px  # This imports plotly.express as px

logging.getLogger('matplotlib.category').setLevel(logging.WARNING)


matplotlib.use('Agg')

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()



def plot_graph(df):
    df['OPERATION_DATE'] = pd.to_datetime(df['OPERATION_DATE'], format='%b-%Y')

# Get the unique kitchen names
    kitchen_names = df['KICHEN_NAME'].unique()

    # Loop through each kitchen and create a chart
    for kitchen in kitchen_names:
        # Filter the DataFrame for the current kitchen
        kitchen_df = df[df['KICHEN_NAME'] == kitchen]
        
        # Create the Plotly chart for this kitchen
        fig = px.line(
            kitchen_df, 
            x='OPERATION_DATE', 
            y='CONSISTENCY', 
            title=f'Consistency per Month for {kitchen}',
            labels={'OPERATION_DATE': 'Month', 'CONSISTENCY': 'Consistency'}
        )
        
        # Update layout for better visuals
        fig.update_layout(
            xaxis_title='Operation Date',
            yaxis_title='Consistency',
            hovermode='x unified'
        )
        
        # Display the figure
        fig.show()  
    
    pass