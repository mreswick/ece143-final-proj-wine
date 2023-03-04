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
# - price
PRICE_COL = 'price'
FREQ_PRICE_COUNT_RES_COL = 'freq_price'
FREQ_PRICE_TABLE = FREQ_PRICE_COUNT_RES_COL
# - province
PROVINCE_COL = 'province'
FREQ_PROVINCE_COUNT_RES_COL = 'freq_province'
FREQ_PROVINCE_TABLE = FREQ_PROVINCE_COUNT_RES_COL
# - region1
REGION1_COL = 'region_1'
FREQ_REGION1_COUNT_RES_COL = 'freq_region1'
FREQ_REGION1_TABLE = FREQ_REGION1_COUNT_RES_COL
# - region2
REGION2_COL = 'region_2'
FREQ_REGION2_COUNT_RES_COL = 'freq_region2'
FREQ_REGION2_TABLE = FREQ_REGION2_COUNT_RES_COL
# - taster_name
TASTER_COL = 'taster_name'
FREQ_TASTER_COUNT_RES_COL = 'freq_taster_name'
FREQ_TASTER_TABLE = FREQ_TASTER_COUNT_RES_COL
# - taster twitter handle
TASTER_TWTR_COL = 'taster_twitter_handle'
FREQ_TASTER_TWTR_COUNT_RES_COL = 'freq_taster_twitter_handle'
FREQ_TASTER_TWTR_TABLE = FREQ_TASTER_TWTR_COUNT_RES_COL
# - variety
VARIETY_COL = 'variety'
FREQ_VARIETY_COUNT_RES_COL = 'freq_variety'
FREQ_VARIETY_TABLE = FREQ_VARIETY_COUNT_RES_COL
# - winery
WINERY_COL = 'winery'
FREQ_WINERY_COUNT_RES_COL = 'freq_winery'
FREQ_WINERY_TABLE = FREQ_WINERY_COUNT_RES_COL


#for testing if imported correctly
def test_import():
  print("Imported!")

#get top n rows of (frequency) table and store in database as new table
def get_top_n_rows(
  cur,
  con,
  col_of_labels,
  col_of_freqs,
  table_name,
  n,
  res_table_name=None,
  other_columns_name="Other"
):
  """Grabs the top n rows, and adds a row at the end
  that is the sum of the remaining rows. It grabs
  these first n rows from table with name table_name,
  and outputs them with same column names in table
  with name res_table_name.
  Params:
   @cur, con: database vars
   @table_name, n res_table_name: name of table to get top n rows from
   and stored in a new table res_table_name.
   @col_of_label: column of names/labels
   @col_of_freqs: column that stores counts of col_of_labels 
   @other_columns_name: the name to put in the non-frequency column
  """
  assert isinstance(col_of_labels, str)
  assert isinstance(col_of_freqs, str)
  assert isinstance(table_name, str)
  assert isinstance(res_table_name, str) or res_table_name == None
  assert isinstance(other_columns_name, str)
  assert isinstance(n, int)
  assert n > 0
  #by default set resulting table name to same as table name but with top_n appended
  res_table_name = res_table_name if res_table_name != None else f'{table_name}_top_{n}'

  freq_top_n_query_str = f'SELECT {col_of_labels}, {col_of_freqs} FROM {table_name} ORDER BY {col_of_freqs} DESC'
  freq_counts = pd.read_sql(freq_top_n_query_str, con)
  freq_counts = freq_counts.set_index(col_of_labels)
  freq_top_n_counts = freq_counts.head(n)
  freq_rest_other = freq_counts.iloc[n:, :]
  freq_rest_other_count = freq_rest_other[col_of_freqs].sum()
  #form resulting dataframe with new row "OTHER" with sum of other count (of rows below n)
  dict_freq_rest_other_row = {}
  dict_freq_rest_other_row[col_of_labels] = [other_columns_name]
  dict_freq_rest_other_row[col_of_freqs] = [freq_rest_other_count]
  freq_rest_other_row_as_df = pd.DataFrame(data=dict_freq_rest_other_row)
  freq_rest_other_row_as_df = freq_rest_other_row_as_df.set_index(col_of_labels)
  #append other row to top n dataframe
  freq_top_n_counts_with_other = pd.concat([freq_top_n_counts, freq_rest_other_row_as_df])
  #write resulting df to database
  freq_top_n_counts_with_other.to_sql(res_table_name, con, if_exists='replace')
  #debug
  # print(freq_top_n_counts_with_other)
  # print(freq_rest_other_row_as_df)
  # print("sum of other: ", freq_rest_other_count)
  # print("sum of counts: ", freq_top_n_counts[col_of_freqs].sum())
  # print("total of counts: ", freq_counts[col_of_freqs].sum())
  #print(freq_top_n_counts)

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
    has that same number of rows. 
  """
  assert isinstance(wine_init_table, str)
  assert isinstance(col_to_count, str)
  assert isinstance(freq_count_res_col, str)
  assert isinstance(freq_count_table, str)
  freq_count_query_str = f'SELECT {col_to_count}, COUNT({col_to_count}) AS {freq_count_res_col} FROM {wine_init_table} GROUP BY {col_to_count} ORDER BY {freq_count_res_col} DESC'
  # two-column table of country names and their counts/frequencies
  freq_counts = pd.read_sql(freq_count_query_str, con)
  # write to database as new table
  freq_counts = freq_counts.set_index(col_to_count)
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

def set_db_freq_province(
  cur,
  con, 
  wine_init_table,
  province_col=PROVINCE_COL, 
  freq_province_count_res_col= FREQ_PROVINCE_COUNT_RES_COL,
  freq_province_table=FREQ_PROVINCE_TABLE,
  testing=False
):
  """Sets the frequency table for the 'points' column from the 
  wine_init_table table. (Two columns, as per
  description of set_db_freq_def.)
  """
  assert isinstance(wine_init_table, str)
  assert isinstance(province_col, str)
  assert isinstance(freq_province_count_res_col, str)
  assert isinstance(freq_province_table, str)
  assert isinstance(testing, bool)
  # if testing, then perform a check to ensure table is created properly
  num_expected_freq_count_rows = None
  set_db_freq_table_def(cur, con, wine_init_table, province_col, 
    freq_province_count_res_col, freq_province_table, num_expected_freq_count_rows)

def set_db_freq_region1(
  cur,
  con, 
  wine_init_table,
  region1_col=REGION1_COL, 
  freq_region1_count_res_col= FREQ_REGION1_COUNT_RES_COL,
  freq_region1_table=FREQ_REGION1_TABLE,
  testing=False
):
  """Sets the frequency table for the 'region1' column from the 
  wine_init_table table. (Two columns, as per
  description of set_db_freq_def.)
  """
  assert isinstance(wine_init_table, str)
  assert isinstance(region1_col, str)
  assert isinstance(freq_region1_count_res_col, str)
  assert isinstance(freq_region1_table, str)
  assert isinstance(testing, bool)
  # if testing, then perform a check to ensure table is created properly
  num_expected_freq_count_rows = None 
  set_db_freq_table_def(cur, con, wine_init_table, region1_col, 
    freq_region1_count_res_col, freq_region1_table, num_expected_freq_count_rows)

def set_db_freq_region2(
  cur,
  con, 
  wine_init_table,
  region2_col=REGION2_COL, 
  freq_region2_count_res_col= FREQ_REGION2_COUNT_RES_COL,
  freq_region2_table=FREQ_REGION2_TABLE,
  testing=False
):
  """Sets the frequency table for the 'region2' column from the 
  wine_init_table table. (Two columns, as per
  description of set_db_freq_def.)
  """
  assert isinstance(wine_init_table, str)
  assert isinstance(region2_col, str)
  assert isinstance(freq_region2_count_res_col, str)
  assert isinstance(freq_region2_table, str)
  assert isinstance(testing, bool)
  # if testing, then perform a check to ensure table is created properly
  num_expected_freq_count_rows = None 
  set_db_freq_table_def(cur, con, wine_init_table, region2_col, 
    freq_region2_count_res_col, freq_region2_table, num_expected_freq_count_rows)

def set_db_freq_taster(
  cur,
  con, 
  wine_init_table,
  taster_col=TASTER_COL, 
  freq_taster_count_res_col= FREQ_TASTER_COUNT_RES_COL,
  freq_taster_table=FREQ_TASTER_TABLE,
  testing=False
):
  """Sets the frequency table for the 'taster_name' column from the 
  wine_init_table table. (Two columns, as per
  description of set_db_freq_def.)
  """
  assert isinstance(wine_init_table, str)
  assert isinstance(taster_col, str)
  assert isinstance(freq_taster_count_res_col, str)
  assert isinstance(freq_taster_table, str)
  assert isinstance(testing, bool)
  # if testing, then perform a check to ensure table is created properly
  num_expected_freq_count_rows = None 
  set_db_freq_table_def(cur, con, wine_init_table, taster_col, 
    freq_taster_count_res_col, freq_taster_table, num_expected_freq_count_rows)

def set_db_freq_taster_twtr(
  cur,
  con, 
  wine_init_table,
  taster_twtr_col=TASTER_TWTR_COL, 
  freq_taster_twtr_count_res_col= FREQ_TASTER_TWTR_COUNT_RES_COL,
  freq_taster_twtr_table=FREQ_TASTER_TWTR_TABLE,
  testing=False
):
  """Sets the frequency table for the 'taster_twitter_handle' column from the 
  wine_init_table table. (Two columns, as per
  description of set_db_freq_def.)
  """
  assert isinstance(wine_init_table, str)
  assert isinstance(taster_twtr_col, str)
  assert isinstance(freq_taster_twtr_count_res_col, str)
  assert isinstance(freq_taster_twtr_table, str)
  assert isinstance(testing, bool)
  # if testing, then perform a check to ensure table is created properly
  num_expected_freq_count_rows = None 
  set_db_freq_table_def(cur, con, wine_init_table, taster_twtr_col, 
    freq_taster_twtr_count_res_col, freq_taster_twtr_table, num_expected_freq_count_rows)

def set_db_freq_variety(
  cur,
  con, 
  wine_init_table,
  variety_col=VARIETY_COL, 
  freq_variety_count_res_col= FREQ_VARIETY_COUNT_RES_COL,
  freq_variety_table=FREQ_VARIETY_TABLE,
  testing=False
):
  """Sets the frequency table for the 'variety' column from the 
  wine_init_table table. (Two columns, as per
  description of set_db_freq_def.)
  """
  assert isinstance(wine_init_table, str)
  assert isinstance(variety_col, str)
  assert isinstance(freq_variety_count_res_col, str)
  assert isinstance(freq_variety_table, str)
  assert isinstance(testing, bool)
  # if testing, then perform a check to ensure table is created properly
  num_expected_freq_count_rows = None 
  set_db_freq_table_def(cur, con, wine_init_table, variety_col, 
    freq_variety_count_res_col, freq_variety_table, num_expected_freq_count_rows)

def set_db_freq_winery(
  cur,
  con, 
  wine_init_table,
  winery_col=VARIETY_COL, 
  freq_winery_count_res_col= FREQ_VARIETY_COUNT_RES_COL,
  freq_winery_table=FREQ_VARIETY_TABLE,
  testing=False
):
  """Sets the frequency table for the 'variety' column from the 
  wine_init_table table. (Two columns, as per
  description of set_db_freq_def.)
  """
  assert isinstance(wine_init_table, str)
  assert isinstance(winery_col, str)
  assert isinstance(freq_winery_count_res_col, str)
  assert isinstance(freq_winery_table, str)
  assert isinstance(testing, bool)
  # if testing, then perform a check to ensure table is created properly
  num_expected_freq_count_rows = None 
  set_db_freq_table_def(cur, con, wine_init_table, winery_col, 
    freq_winery_count_res_col, freq_winery_table, num_expected_freq_count_rows)