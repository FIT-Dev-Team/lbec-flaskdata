import pandas as pd
import plotly.express as px
import plotly.io as pio

def plot_graph(df):
    df['OPERATION_DATE'] = pd.to_datetime(df['OPERATION_DATE'], format='%b-%Y')

    # Get the unique kitchen names
    kitchen_names = df['KICHEN_NAME'].unique()

    # List to store HTML for each chart
    charts_html = []

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
            labels={'OPERATION_DATE': 'Month', 'CONSISTENCY': 'Consistency'},
            text='CONSISTENCY'  # Add data labels (this shows consistency values)
        )
        
        # Update layout and traces for better visuals
        fig.update_layout(
            xaxis_title='Operation Date',
            yaxis_title='Consistency',
            hovermode='x unified',
            xaxis=dict(
                tickformat='%b %Y',  # Format ticks as 'Month Year'
                dtick='M1',          # Set the tick interval to 1 month
            )
        )
        
        # Update traces to ensure data labels are shown
        fig.update_traces(
            textposition='top center',  # Position data labels at the top of each point
            texttemplate='%{text:.2f}',  # Format the text to show up to 2 decimal places
            mode='lines+markers+text',   # Ensure both lines, markers, and text are shown
            marker=dict(size=8),         # Increase the size of the markers for better visibility
        )
        # Convert the figure to an HTML string
        fig_html = pio.to_html(fig, full_html=False)
        charts_html.append(fig_html)
    
    # Return the list of HTML chart strings
    return charts_html