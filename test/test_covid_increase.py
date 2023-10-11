import datetime

import pandas as pd
from google.cloud.bigquery import Client
from sql_test_kit.query_interpolation import QueryInterpolator

from sql_test_kit_example.covid_increase import covid_table, covid_increase_query


def test_covid_increase_query():
	# Given
	covid_data = pd.DataFrame(
		{
			"date": [
				datetime.date(2020, 3, 21),
				datetime.date(2020, 3, 22),
				datetime.date(2020, 3, 23)
			],
			"country_name": ["United States of America"] * 3,
			"cumulative_confirmed": [10, 20, 30],
		}
	)
	interpolated_query = QueryInterpolator() \
		.add_input_table(covid_table, covid_data) \
		.interpolate_query(covid_increase_query)

	# When
	covid_increase_data = Client().query(interpolated_query).to_dataframe()

	# Then
	expected_covid_increase_data = pd.DataFrame(
		{
			"Date": [datetime.date(2020, 3, 23)],
			"Confirmed_Cases_On_Day": [30],
			"Confirmed_Cases_Previous_Day": [20],
			"Percentage_Increase_In_Cases": [50],
		}
	)
	pd.testing.assert_frame_equal(
		covid_increase_data,
		expected_covid_increase_data,
		check_dtype=False
	)
