import pandas as pd

"""Get statistics about frequencies in columns.
"""

#constants
# column names:
COUNTRY_COL = "country"
FREQ_COUNTRY_COUNT_RES_COL = "freq_country"
FREQ_COUNTRY_TABLE = FREQ_COUNTRY_COUNT_RES_COL

#for testing if imported correctly
def test_import():
  print("Imported!")

def set_db_freq_country(
  cur,
  con, 
  wine_init_table,
  country_col=COUNTRY_COL, 
  freq_country_count_res_col= FREQ_COUNTRY_COUNT_RES_COL,
  freq_country_table=FREQ_COUNTRY_TABLE,
  testing=True):
  """Outputs the frequency of each country in the country column of
  the wine database into a new two-column table whose first column is the country
  and the second its count.
  Params:
    @con: connection to the wine init database
    @wine_init_table: the name of the wine init table
    @country_col: the name of the column in the wine init database for the countries of wine
    @freq_country_count_res_col: the name for the resulting column to store
    the counts of countries. 
    @freq_country_table: the name of the resulting table to store the frequencies 
    of the countries.
  The reuslting output table is put in the same wine init database as the con passed in,
  and has two columns: 
    - COUNTRY_COL: the name of the country;
    - FREQ_COUNTRY_COUNT_RES_COL: the count of that country
  """
  assert isinstance(wine_init_table, str)
  assert isinstance(country_col, str)
  assert isinstance(freq_country_count_res_col, str)
  assert isinstance(freq_country_table, str)
  assert isinstance(testing, bool)
  country_count_query_str = f'SELECT {country_col}, COUNT({country_col}) AS {freq_country_count_res_col} FROM {wine_init_table} GROUP BY {country_col}'
  # two-column table of country names and their counts/frequencies
  country_counts = pd.read_sql(country_count_query_str, con)
  # write to database as new table
  country_counts.to_sql(freq_country_table, con, if_exists='replace') 
  # if testing, then perform a check to ensure table is created properly
  if(testing):
    #number of rows expected in freq country table
    NUM_EXPECTED_FREQ_COUNTRY_ROWS = 44 #43 if excluding a "None" row
    #get number of current rows
    num_freq_country_rows = cur.execute("SELECT COUNT(*) FROM " + freq_country_table).fetchall()[0][0]
    assert NUM_EXPECTED_FREQ_COUNTRY_ROWS == num_freq_country_rows