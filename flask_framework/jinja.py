### Building URL dynamically
### Jinja2 template engine

from flask import Flask, render_template, request
import pandas as pd
from databricks.sql import connect
from dotenv import load_dotenv
import os

load_dotenv()
app=Flask(__name__)

@app.route('/')
def welcome():
    return render_template(
        'welcome.html',
        table_list=['enriched_sessions', 'enriched_histo']
    )

@app.route('/describe_table/<table_name>')
def describe_table(table_name):
    connection = connect(
        server_hostname=os.getenv("DATABRICKS_SERVER_HOSTNAME"),
        http_path=os.getenv("DATABRICKS_HTTP_PATH"),
        access_token=os.getenv("DATABRICKS_ACCESS_TOKEN")
    )
    if table_name not in ['enriched_sessions', 'enriched_histo']:
        return f'<h1>Table {table_name} does not exist in database.</h1>'
    query = f"DESCRIBE `du_stats`.`gold`.`{table_name}`"
    description_df = pd.read_sql(query,connection)
    description_html = description_df.to_html(index=False)
    query = f"""
    SELECT
        '{table_name}' AS table_name,
        COUNT(*) AS num_rows
    FROM `du_stats`.`gold`.`{table_name}`
    """
    row_count_df = pd.read_sql(query,connection)
    row_count_html=row_count_df.to_html(index=False)
    query = f"""
    SELECT
        site_name,
        COUNT(DISTINCT log_date) AS days_logged,
        MIN(log_date) AS first_log_date,
        MAX(log_date) AS last_log_date
    FROM `du_stats`.`gold`.`{table_name}`
    GROUP BY site_name
    """
    site_info_df = pd.read_sql(query,connection)
    site_info_html=site_info_df.to_html(index=False)
    query = f"""
    SELECT
        log_date,
        site_name,
        COUNT(DISTINCT ueid) AS number_of_ue
    FROM `du_stats`.`gold`.`{table_name}`
    GROUP BY site_name, log_date
    ORDER BY log_date DESC, site_name ASC
    """
    ue_info_df = pd.read_sql(query,connection)
    ue_info_html=ue_info_df.to_html(index=False)
    return render_template(
        'table_description.html',
        table_name=table_name,
        table_description=description_html,
        table_rows=row_count_html,
        site_info=site_info_html,
        ue_info=ue_info_html
    )

if __name__ == '__main__':
    app.run(debug=True)