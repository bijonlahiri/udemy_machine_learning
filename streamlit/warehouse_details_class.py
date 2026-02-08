from databricks.sql import connect
import os
import pandas as pd
from dotenv import load_dotenv

class WarehouseDetails:
    def __init__(self):
        load_dotenv()
        self.connection = connect(
            server_hostname=os.getenv("DATABRICKS_SERVER_HOSTNAME"),
            http_path=os.getenv("DATABRICKS_HTTP_PATH"),
            access_token=os.getenv("DATABRICKS_ACCESS_TOKEN")
        )

    def get_tables(self):
        query = "SHOW TABLES IN du_stats.gold"
        tables_df = pd.read_sql(query, self.connection)
        return tables_df['tableName'].tolist()

    def get_table_description(self, table):
        query = f"DESCRIBE `du_stats`.`gold`.`{table}`"
        description_df = pd.read_sql(query, self.connection, index_col='col_name')
        return description_df

    def get_row_count(self, table):
        query = f"""
        SELECT
            log_date,
            COUNT(*) AS num_rows
        FROM `du_stats`.`gold`.`{table}`
        WHERE YEAR(log_date) <= YEAR(CURRENT_DATE)
        GROUP BY log_date
        ORDER BY log_date DESC
        """
        row_count_df = pd.read_sql(query, self.connection, index_col='log_date')
        return row_count_df

    def get_site_information(self, table):
        query = f"""
        SELECT
            site_name,
            COUNT(DISTINCT log_date) AS days_logged,
            MIN(log_date) AS first_log_date,
            MAX(log_date) AS last_log_date
        FROM `du_stats`.`gold`.`{table}`
        WHERE YEAR(log_date) <= YEAR(CURRENT_DATE)
        GROUP BY site_name
        """
        site_info_df = pd.read_sql(query, self.connection, index_col='site_name')
        return site_info_df
    
    def get_ue_information(self, table):
        query = f"""
            SELECT
                log_date,
                site_name,
                COUNT(DISTINCT ueid) AS number_of_ue
            FROM `du_stats`.`gold`.`{table}`
            WHERE YEAR(log_date) <= YEAR(CURRENT_DATE)
            GROUP BY site_name, log_date
            ORDER BY log_date DESC, site_name ASC
        """
        ue_info_df = pd.read_sql(query, self.connection, index_col='log_date')
        return ue_info_df
    
    def get_session_information(self, table):
        query = f"""
        SELECT
            log_date,
            site_name,
            COUNT(DISTINCT session_id) AS number_of_sessions,
            AVG(session_duration) AS avg_session_duration,
            MIN(session_duration) AS min_session_duration,
            MAX(session_duration) AS max_session_duration
        FROM `du_stats`.`gold`.`{table}`
        WHERE YEAR(log_date) <= YEAR(CURRENT_DATE)
        GROUP BY site_name, log_date
        ORDER BY log_date DESC, site_name ASC
        """
        session_info_df = pd.read_sql(query, self.connection, index_col='log_date')
        return session_info_df