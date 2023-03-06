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