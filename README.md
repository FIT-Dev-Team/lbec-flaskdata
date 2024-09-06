# Data Consistency and Food Waste Management App

This is a Flask web application designed for internal use within our organization to manage and analyze food waste and data consistency for various kitchens. The app fetches data from our internal database and processes it to generate tables, graphs, and exportable data.

### **Note:**
This application requires credentials to access the internal database and is not intended for public use. Only users with valid database credentials can run the app locally. 
 
**To use the site, go to [\[Website Link\]](https://flaskdata-0dvn.onrender.com)**.  
Please note that due to free hosting, the site may take up to **50 seconds to load**. 😉

---

## Features

- **Total Food Waste**: Generate CSV file summarizing food waste amounts across categories and food types for a given date range. Plate waste excluded.
- **Entry**: Get raw data of food waste and covers including amount of entries for a selected company and date range.
- **Data Consistency**: Analyze data consistency over time, with breakdowns by kitchen and month.

### Available Forms:
- `/form_total_fw`: Generate total food waste reports.
- `/form_entries`: Generate deta entries for food waste and covers.
- `/form_dcon`: Generate data consistency reports over time.

### Data Outputs:
- **CSV** or **Excel** downloads of reports for total food waste, data consistency, and entry counts.
- **Interactive Charts** showing monthly trends for food waste and data consistency.
- **PNG** downloads of graphs through Plotly.

---

## Installation

To run the application locally, follow these steps:

1. Clone this repository to your local machine:

    ```bash
    git clone https://github.com/FIT-Dev-Team/lbec-flaskdata.git 
    ```

2. Install the required dependencies:

    ```bash
    pip install -r requirements.txt
    ```

3. Set up environment variables in a `.env` file. The following environment variables are required:
    - `user`
    - `password`
    - `host`
    - `database`

4. Ensure you have access to the MySQL database with appropriate credentials. Update the `.env` file with your database credentials.

5. Run the Flask app:

    ```bash
    python app.py
    ```

The app will run on `http://localhost:10000`.

---

## How to Use

The application provides an easy-to-use web interface to generate reports and analyze food waste and data consistency. Here's how to use it:

### 1. **Home Page**
- When you visit the homepage, you'll see three main options: 
  - **Total FW**
  - **FW & CV Entries**
  - **Data Consistency**
- Click on the desired option to proceed to the corresponding form.

### 2. **Total Food Waste Report**
- Navigate to the **Total FW** section by clicking on it in the navigation bar or from the home page.
- **Steps**:
  1. Enter the **Company Name**.
  2. Select a **Start Date** and an **End Date** for the report.
  3. Click **Submit** to download a **CSV file**.

### 3. **FW & CV Entries**
- Navigate to the **FW & CV Entries** section.
- **Steps**:
  1. Enter the **Company Name**.
  2. Select a **Start Date** and an **End Date** for the report.
  3. Click **Submit** to download an **Excel file**, with separate sheets for:
  - **Food Waste (FW) entries**
  - **Covers (CV) entries**
  - **Food Waste Entry Counts** by kitchen.

### 4. **Data Consistency**
- Navigate to the **Data Consistency** section.
- **Steps**:
  1. Enter the **Company Name**.
  2. Select an **End Date** (Data consistency is counted from after the first baseline).
  3. Click **Submit** to generate the monthly & overall data consistency report.
- This will provide a detailed report on how consistent the data entries have been over time.
- The report includes interactive **graphs** and can be downloaded as an **Excel file** for tables and **PNG file** for **graphs**.

---

### Additional Notes:
- **Load Time**: Due to free hosting, the website may take up to **50 seconds** to load. Please be patient while the data is fetched and processed.
- **File Downloads**: Each section provides the processed data as a **CSV** or **Excel** file, which can be useful for further analysis.

---

## Future Enhancements

### 1. **Monthly Grams per Cover (g/Cover) Report**
- A new report for **monthly grams per cover (g/Cover)** will be introduced.
- The report will include:
  - **Tables** showing the monthly g/Cover data, available for download in **Excel** format.
  - **Interactive Graphs** displaying trends over time, which can be downloaded in **PNG** format (via Plotly).
- This feature will align with the current **Data Consistency** report setup, providing users with both visual and tabular insights.

---

## Troubleshooting

- **Slow Loading Times**: If the web app takes too long to load, it could be due to the free hosting provider's limitations. Please allow up to **50 seconds** for the site to load.
- **Database Access**: Ensure you have valid credentials in your `.env` file to access the internal MySQL database.

---

## License

This project is for internal use only and is not licensed for public distribution.
