import pandas as pd

"""Get statistics about frequencies in columns.
"""

#constants
# column names:
# - country:
COUNTRY_COL = "country"
FREQ_COUNTRY_COUNT_RES_COL = "freq_country"
FREQ_COUNTRY_TABLE = FREQ_COUNTRY_COUNT_RES_COL
# - desigmation:
DESIG_COL = 'designation'
FREQ_DESIG_COUNT_RES_COL = 'freq_designation'
FREQ_DESIG_TABLE = FREQ_DESIG_COUNT_RES_COL
# - points:
POINTS_COL = 'points'
FREQ_POINTS_COUNT_RES_COL = 'freq_points'
FREQ_POINTS_TABLE = FREQ_POINTS_COUNT_RES_COL

#for testing if imported correctly
def test_import():
  print("Imported!")

def set_db_freq_table_def(
  cur,
  con,
  wine_init_table,
  col_to_count,
  freq_count_res_col,
  freq_count_table,
  num_expected_freq_count_rows=None  
):
  """A defualt function to create a two-column table in the given database (given by con and cur)
  that gives the frequency 
  of each distinct entry in the selected column.
  Outputs a table with name freq_count_table in the
  database given by cur and con with two columns whose names
  are given by col_to_count and freq_count_res_col.
  Param::
    @cur, @con: vars for that database (should be for the wine init database)
    @wine_init_table: the name of the wine init table
    @col_to_count: the name of the column in the database to count / get the frequencies of
    @freq_count_res_col: the name of the resulting column of counts
    @freq_count_table: the name to give to this resulting freuquency table.
    @num_expected_freq_count_rows: used for testing purposes: is the number of 
    rows the resulting two-columned table is expected to have. If specified,
    then this function also performs a test to assert that the resulting table
    has that same name of rows. 
  """
  assert isinstance(wine_init_table, str)
  assert isinstance(col_to_count, str)
  assert isinstance(freq_count_res_col, str)
  assert isinstance(freq_count_table, str)
  freq_count_query_str = f'SELECT {col_to_count}, COUNT({col_to_count}) AS {freq_count_res_col} FROM {wine_init_table} GROUP BY {col_to_count}'
  # two-column table of country names and their counts/frequencies
  freq_counts = pd.read_sql(freq_count_query_str, con)
  # write to database as new table
  freq_counts.to_sql(freq_count_table, con, if_exists='replace') 
  # if testing, then perform a check to ensure table is created properly
  if(num_expected_freq_count_rows):
    #get number of current rows
    num_freq_count_rows = cur.execute("SELECT COUNT(*) FROM " + freq_count_table).fetchall()[0][0]
    assert num_expected_freq_count_rows == num_freq_count_rows

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
    @con, cur: connection vars to the wine init database
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
  # if testing, then perform a check to ensure table is created properly
  num_expected_freq_count_rows = 44 if testing else None #43 if excluding a "None" row
  set_db_freq_table_def(cur, con, wine_init_table, country_col, 
    freq_country_count_res_col, freq_country_table, num_expected_freq_count_rows)


"""
NOTE TO-DO: 
some NLP needs to be done here (for instance, currently,
'10 Anos Old Tawny', '10 Year Tawny', '10 Year Old Tawny', etc.,
are all being interpreted as separate designations.)
""" 
def set_db_freq_desig(
  cur,
  con, 
  wine_init_table,
  desig_col=DESIG_COL, 
  freq_desig_count_res_col= FREQ_DESIG_COUNT_RES_COL,
  freq_desig_table=FREQ_DESIG_TABLE,
  testing=False
):
  """Sets the frequency table for the 'designation' column from the 
  wine_init_table table. (Two columns, as per
  description of set_db_freq_def.)
  """
  assert isinstance(wine_init_table, str)
  assert isinstance(desig_col, str)
  assert isinstance(freq_desig_count_res_col, str)
  assert isinstance(freq_desig_table, str)
  assert isinstance(testing, bool)
  # if testing, then perform a check to ensure table is created properly
  num_expected_freq_count_rows = 37980 if testing else None #37979 if excluding a "None" row
  set_db_freq_table_def(cur, con, wine_init_table, desig_col, 
    freq_desig_count_res_col, freq_desig_table, num_expected_freq_count_rows)

def set_db_freq_points(
  cur,
  con, 
  wine_init_table,
  points_col=POINTS_COL, 
  freq_points_count_res_col= FREQ_POINTS_COUNT_RES_COL,
  freq_points_table=FREQ_POINTS_TABLE,
  testing=False
):
  """Sets the frequency table for the 'points' column from the 
  wine_init_table table. (Two columns, as per
  description of set_db_freq_def.)
  """
  assert isinstance(wine_init_table, str)
  assert isinstance(points_col, str)
  assert isinstance(freq_points_count_res_col, str)
  assert isinstance(freq_points_table, str)
  assert isinstance(testing, bool)
  # if testing, then perform a check to ensure table is created properly
  num_expected_freq_count_rows = 21 if testing else None  
  set_db_freq_table_def(cur, con, wine_init_table, points_col, 
    freq_points_count_res_col, freq_points_table, num_expected_freq_count_rows)