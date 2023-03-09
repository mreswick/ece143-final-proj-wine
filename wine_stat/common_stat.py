import pandas as pd

"""A general-purpose statistics module,
and other basic operations (like number
of rows in table)."""

def get_num_rows_in_table(cur, con, table_name):
  """Gets the number of rows in the given table with name
  table_name. Includes null entries and duplicate rows
  in its count (ie it also counts those).
  Param:
    @cur, con: database connection vars
    @table_name: name of the table to return the number of rows of
  """
  COUNT_COL_RES_NAME = "COUNT_COL"
  pd_table = pd.read_sql(f'SELECT COUNT(*) AS {COUNT_COL_RES_NAME} FROM {table_name}', con)
  total_count = pd_table[COUNT_COL_RES_NAME][0]
  return total_count

def get_num_unique_rows_in_column(cur, con, table_name, col_name):
  """Gets the number of non-null distinct/unique rows in the given
  column in that table; returns this as an integer. Null entries
  are thus ignored / not included in this count.
  Param:
   @cur, con: database vars
   @table_name: name of the table to use
   @col_name: column in table to se
  """
  COUNT_DISTINCT_COL_RSE_NAME = "COUNT_COL_DISTINCT"
  pd_table = pd.read_sql(f'SELECT COUNT(DISTINCT {col_name}) AS {COUNT_DISTINCT_COL_RSE_NAME} FROM {table_name}', con)
  total_distinct_count = pd_table[COUNT_DISTINCT_COL_RSE_NAME][0]
  return total_distinct_count 

def get_num_uniq_rows_for_each_col_in_table(cur, con, table_name):
  """Gets the number of non=null distinct/unique rows for
  each column in that table; returns this as a dictionary
  with the keys as column names and the values as these
  counts.
  Param:
    @cur, con: database vars
    @table_name: the name of the table to get the number
    of unique entries in each column for
  """ 
  pd_table = pd.read_sql(f'SELECT * FROM {table_name}', con)
  pd_table.set_index('index')
  col_names = list(pd_table.columns.values)
  dict_uniq_row_counts = {}
  for col_name in col_names:
    if col_name != 'index': # don't count values for index column
      dict_uniq_row_counts[col_name] = get_num_unique_rows_in_column(cur,
        con, table_name, col_name)
  return dict_uniq_row_counts

def get_basic_stats_of_col1_grouped_by_cols(cur, con,
  col_name, cols_to_group_by, input_table_name, res_table_suffix="",
  stats_to_compute=['count', 'min', 'max', 'mean', 'median', 'std']):
  """Creates a new table from the input table name,
  grouped by the column names passed, that gives the statistical
  functions passed computed on that column by those groups, and writes
  this new table to the database. Currently the name of this table
  is auto-generated here, but you can append to this name
  if you like via res_table_suffix.
  Param:
   @cur, con: database vars
   @col_name: the numeric column to calculate basic statistics
   on. Note that this column must be numeric for this function
   to work.
   @cols_to_group_by: a list of column names (as strings) to 
   group by. Note that the order of column names in this list
   matters: values are grouped in the order from first element
   of this list to last element.
   @input_table_name: the name of the table these
   columns for col_name and cols_to_group_by are in
   @res_table_suffix: a suffix to add to the end of the
   resulting table created with these statistics computed
   on col_name
   @stats_to_compute: a list of functions to compute statistics
   for on that numeric column col_name. If implemented in pandas,
   then that function can be its string function name.
    - For more info on what to pass to stats_to_compute,
      look at the info for pandas.DataFrame.aggregate:
      https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.aggregate.html.
      This argument is just the list of functions argument to
      aggregate(), so whatever goes for that list for it also
      applies here on what stats_to_compute may be.
  The resulting table has the columns for statistics have
  the same names as the names of the functions used to compute them
  (currently assumes pandas functions, though might work otherwise).
  Note that this function will overwrite a table of the same name
  if it already exists.
   
  Ex: if table_name is the table name for the null-cleaned
  overall wine table (currently wine_init), 
  the column to calculate these statistics on is
  col_name='price', and the columns to group by is
  ['country', 'region_1'], with no res_table_suffix
  specified, then this function creates a table named
  'price_basic_stats_grouped_by_country_region_1'
  with columns 'country' and 'region_1' followed
  by the stats for each. If res_table_suffix were
  'stats_table', for example, then the resulting table
  name for the created table would instead be
  'price_basic_stats_grouped_by_country_region_1_stats_table',
  for example.

   Returns the name (as a string) of this newly-created 
   table of basics statistics.
  """ 
  assert isinstance(col_name, str)
  assert isinstance(cols_to_group_by, list)
  for col in cols_to_group_by:
    assert isinstance(col, str)
  assert isinstance(input_table_name, str)
  assert isinstance(res_table_suffix, str)

  #construct resulting table name for stats in database to create
  str_cols_grouped_by = ""
  for col_to_group_by in cols_to_group_by:
    str_cols_grouped_by += "_" + col_to_group_by
  # add undescore for result table name's suffix if it is specified
  res_table_suffix = res_table_suffix if not(res_table_suffix) else f'_{res_table_suffix}' 
  res_table_name = f'{col_name}_basic_stats_grouped_by{str_cols_grouped_by}'

  #read in table to data frame
  pd_table = pd.read_sql(f'SELECT * FROM {input_table_name}', con)
  #group by the columns in cols_to_group_by
  pd_res = pd_table.groupby(cols_to_group_by)[col_name].aggregate(stats_to_compute)
  #output to database
  pd_res.to_sql(res_table_name, con, if_exists='replace')


def get_common_numeric_stats_by_table(cur, con, input_table_name):
  """
  create numeric stats for the input table, non-numeric cols will not get 
  stats here(use freq for those cols)
  Param:
   @cur, con: database vars
   @input_table_name: the name of the table

   Returns the name (as a string) of this newly-created 
   table of basics statistics.

  """
  assert isinstance(input_table_name, str)
  #create save name for stats table
  res_table_name = f'{input_table_name}_numeric_stats'
  #read in table to data frame
  pd_table = pd.read_sql(f'SELECT * FROM {input_table_name}', con, index_col='index')
  #stats for the table
  pd_res = pd_table.describe()
  #output to database
  pd_res.to_sql(res_table_name, con, if_exists='replace')
  
  return pd_res


  