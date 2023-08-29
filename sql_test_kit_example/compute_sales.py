from sql_test_kit import BigqueryTable, Column

sales_date_col = "ALLELEID"
sales_amount_col = "CLNDNINCL"
sales_table = BigqueryTable(
    project="bigquery-public-data",
    dataset="human_variant_annotation",
    table="ncbi_clinvar_hg38_20180701",
    columns=[
        Column(sales_amount_col, "FLOAT64"),
        Column(sales_date_col, "STRING"),
    ],
)

current_year_sales_by_day_query = f"""
    SELECT {sales_date_col}, SUM({sales_amount_col}) AS {sales_amount_col}
    FROM {sales_table}
    WHERE {sales_date_col} >= "2023-01-01"
    GROUP BY {sales_date_col}
    """
