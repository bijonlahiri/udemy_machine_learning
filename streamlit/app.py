import streamlit as st
from warehouse_details_class import WarehouseDetails
#initialize the object of WarehouseDetails class
warehouse_details = WarehouseDetails()

# Title of the application
st.title("Details of Databricks SQL Warehouse")

# Display a simple text
st.write("This application provides details regarding the Databricks SQL warehouse")

# Fetch list of tables
tables_list = warehouse_details.get_tables()
table = st.selectbox(
    "Select a table to view it's details:",
    tables_list,
    index=None,
    placeholder="Select a table"
)
if table:
    st.subheader(f"Details of table: {table}")

    # Describe table
    st.markdown("### Table Description")
    st.dataframe(warehouse_details.get_table_description(table))

    # Row count
    st.markdown("### Row Count")
    st.dataframe(warehouse_details.get_row_count(table))

    # Site information
    st.markdown("### Site Information")
    st.dataframe(warehouse_details.get_site_information(table))

    # UE information
    st.markdown("### UE Information")
    st.dataframe(warehouse_details.get_ue_information(table))

    # Session information
    if table in ['enriched_sessions', 'ue_session']:
        st.markdown('### Session Information')
        st.dataframe(warehouse_details.get_session_information(table))