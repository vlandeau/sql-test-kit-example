from sql_test_kit import Column, BigqueryTable


covid_table = BigqueryTable(
	project="bigquery-public-data",
	dataset="covid19_open_data",
	table="covid19_open_data",
	columns=[
		Column("date", "DATE"),
		Column("country_name", "STRING"),
		Column("cumulative_confirmed", "INTEGER"),
	]
)


covid_increase_query = f"""
    WITH us_cases_by_date AS (
      SELECT
        date,
        SUM( cumulative_confirmed ) AS cases
      FROM
        `bigquery-public-data.covid19_open_data.covid19_open_data`
      WHERE
        country_name="United States of America"
        AND date between '2020-03-22' and '2020-04-20'
      GROUP BY
        date
      ORDER BY
        date ASC
     ), 
     us_previous_day_comparison AS
    (SELECT
      date,
      cases,
      LAG(cases) OVER(ORDER BY date) AS previous_day,
      cases - LAG(cases) OVER(ORDER BY date) AS net_new_cases,
      (cases - LAG(cases) OVER(ORDER BY date)) * 100 / LAG(cases) OVER(ORDER BY date) AS percentage_increase
    FROM us_cases_by_date
    )
    SELECT
      Date,
      cases AS Confirmed_Cases_On_Day,
      previous_day AS Confirmed_Cases_Previous_Day,
      percentage_increase AS Percentage_Increase_In_Cases
    FROM
      us_previous_day_comparison
    WHERE
      percentage_increase > 10
"""
