import pandas as pd
from google.cloud.bigquery import Client

from sql_test_kit import QueryInterpolator
from sql_test_kit_example.compute_sales import sales_date_col, sales_amount_col, sales_table, \
    current_year_sales_by_day_query


def test_current_year_sales_by_day_query():
    # Given
    sales_data = pd.DataFrame(
        {
            "SALES_ID": [1, 2, 3, 4],
            sales_date_col: ["2022-12-31", "2023-01-01", "2023-01-01", "2023-01-02"],
            sales_amount_col: [10, 20, 30, 40],
        }
    )

    # When
    interpolated_query = QueryInterpolator() \
        .add_input_table(sales_table, sales_data) \
        .interpolate_query(current_year_sales_by_day_query)
    current_year_sales_by_day_data = Client().query(interpolated_query).to_dataframe()

    # Then
    expected_current_year_sales_by_day_data = pd.DataFrame(
        {
            sales_date_col: ["2023-01-01", "2023-01-02"],
            sales_amount_col: [50, 40],
        }
    )

    pd.testing.assert_frame_equal(
        current_year_sales_by_day_data,
        expected_current_year_sales_by_day_data,
        check_dtype=False,
    )
