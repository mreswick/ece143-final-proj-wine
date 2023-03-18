import pandas as pd
from wine_stat import common_stat

"""A module for getting null-related info about a table.
Namely, use this module by simply calling "print_null_info_for_table"
below for the specified table, as this will print out a summary
of all of the null-related info for that table."""

def print_null_info_for_table(cur, con, table_name):
  """Prints out (on the command-line) the null info for
  each column. It prints five pieces of info:
    1. the total number of rows in the table (including null entries)
    2. the total number of null entries in each column
    3. the percentage of entries in each column that are null
    4. the total number of rows with no null entries in the table
    5. the percentage of rows with no null entries in the table
  THIS FUNCTION DOES NOT RETURN ANYTHING, it just prints null
  info for the table.
  Param:
    @cur, con: database vars
    @table_name: the table to print the total number of rows and null info for
  """ 
  total_num_rows_in_table = common_stat.get_num_rows_in_table(cur, con, table_name)
  dict_num_nulls_in_each_col = get_num_nulls_in_each_column(cur, con, table_name)
  dict_percentage_null_in_each_column = get_percentage_null_in_each_column(cur, con, table_name)
  total_num_no_nulls_rows_in_table = get_num_fully_non_null_rows_for_table(cur, con, table_name)
  #get percentage of rows with no nulls rounded to two decimal places
  percent_rows_no_nulls = round(100 * (total_num_no_nulls_rows_in_table/total_num_rows_in_table), 2)
  #print summary info
  print("*************")
  print("Null info for table with name: ", table_name)
  print(" - The total number of rows of this table is: ", f'{total_num_rows_in_table} rows')
  print(" - The number of nulls in each column is given by: ", dict_num_nulls_in_each_col)
  print(" - The percentage of nulls in each column (rounded to two decimal places) is given by: ", dict_percentage_null_in_each_column)
  print(" - The number of rows with no null entries is: ", f'{total_num_no_nulls_rows_in_table} rows')
  print(" - The percentage of rows in the table with no null entries is: ", f'{percent_rows_no_nulls}%')
  print("*************")

def get_num_fully_non_null_rows_for_table(cur, con, table_name):
  """Returns the number of rows that have no null entries
  in them for the given table.
  Param:
    @cur, con: database vars
    @table_name: the table ot get the number of fully non-null
    rows for
  """
  pd_table = pd.read_sql(f'SELECT * FROM {table_name}', con)
  pd_table.set_index('index')
  #all column names of table
  col_names = list(pd_table.columns.values)
  assert len(col_names) > 0 #assert table has at least one column
  #build query string to select only those rows with
  #no null entries
  query_str = f'SELECT * FROM {table_name} WHERE ' #{col_names[0]} IS NOT NULL'
  i = 0
  for col_name in col_names:
    if col_name != 'index': #don't consider the index column
      if i == 0: #if first entry, then don't prefix expression with ' AND '
        query_str += f'{col_name} IS NOT NULL'
      else:
        query_str += f' AND {col_name} IS NOT NULL'
      i += 1
  pd_no_nulls_table = pd.read_sql(query_str, con)
  #number of rows for this no-nulls table
  num_rows_no_nulls = pd_no_nulls_table.shape[0]
  return num_rows_no_nulls

def get_percentage_null_in_each_column(cur, con, table_name):
  """Returns a dictionary of "columns: percentages" that gives
  the percent of values of each column that are null.
  Ex: if table has columns col1, col2, and 50% of entries
  in col1 are null, and 67.5% of entries in col2 are null,
  then this function returns the dictionary:
    {col1: 50, col2: 67.5}
  Param:
    @cur, con: database vars
    @table_name: the table to get the percentage of nulls
  """
  total_num_rows_in_table = common_stat.get_num_rows_in_table(cur, con, table_name)
  dict_num_nulls_in_each_col = get_num_nulls_in_each_column(cur, con, table_name)
  dict_percentage_null_in_each_column = {} #dictionary to return
  for (col_name, num_rows_null) in dict_num_nulls_in_each_col.items():
    #round percentage to two decimal places
    percent_null_in_col = round(100 * (num_rows_null/total_num_rows_in_table), 2)
    dict_percentage_null_in_each_column[col_name] = percent_null_in_col
  return dict_percentage_null_in_each_column

def get_num_nulls_in_each_column(cur, con, table_name):
  """Returns a dictionary of the column names to their
  number of null entries for that table with name
  table_name. Ie, counts the number of nulls in each column
  of table table_name, returned as a dictionary.
  Ex: if table has columns col1, col2, and a is the number
  of null entries in col1 and b the number of null entries
  in col2, then this function returns the dictionary:
    {col1: a, col2: b}
  Param:
    @cur, con: database vars
    @table_name: the table to get the counts of the null columns in
  """ 
  pd_table = pd.read_sql(f'SELECT * FROM {table_name}', con)
  pd_table.set_index('index')
  #get all column names of pandas dataframe for table
  col_names = list(pd_table.columns.values)
  COUNT_COL_RES = "COUNT_COL" # name of resulting count column just below
  dict_count_col_nulls = {} # dictionary to return
  for col_name in col_names:
    if col_name != 'index': #don't count the index column
      query_str = f'SELECT COUNT(*) AS {COUNT_COL_RES} FROM {table_name} WHERE {col_name} IS NULL'
      count_nulls_in_col = pd.read_sql(query_str, con)
      dict_count_col_nulls[col_name] = count_nulls_in_col[COUNT_COL_RES][0] #grab 1st/only entry: the count entry
  return dict_count_col_nulls