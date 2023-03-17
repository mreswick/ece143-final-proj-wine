import pandas as pd

"""Common operations with database."""


def read_table(cur, con, tablename, cols=[],
  col_to_sort_by=None, sort_by_ascending=False):
  """Reads the table from the database,
  returning all columns by default,
  else the concatenated list given
  of column names in the order
  given in that list.
  Param:
    @cur, con: database vars
    @tablename: the name of the table to read from
    @cols: the list of columns to select
  """
  assert isinstance(cols, list)
  for i in cols:
    assert isinstance(i, str)
  assert isinstance(tablename, str)
  if col_to_sort_by != None:
    assert isinstance(col_to_sort_by, str)
  assert isinstance(sort_by_ascending, bool)

  #build query str
  query_str = ''
  if len(cols) == 0:
    query_str = f'SELECT * FROM {tablename}'
  else:
    query_str = f'SELECT {cols[0]}'
    for col in cols[1:]:
      query_str += f', {col}'
      query_str += f' FROM {tablename}'
  df = pd.read_sql(query_str, con)
  if col_to_sort_by != None:
    df = df.sort_values(by=col_to_sort_by, ascending=sort_by_ascending)
  return df

def join_dfs(df1, df2, list_cols_to_join_on, join_type='inner', **kwargs):
  return df1.merge(df2, on=list_cols_to_join_on, how=join_type, **kwargs)

def filter_top_n(
  cur,
  con,
  cols_to_group_by,
  col_to_measure_by,
  topnnum,
  df=None,
  df_table_name=None,
  df_to_measure_by=None,
  tablename_to_measure_by=None,
  ascending=False
):
  """Grabs rows from the given df 
  that are in the given topnnum by
  col_to_measure_by for those columns
  grouped by in the dataframe
  or sql table to measure by.
  Only one of 
  df_to_measure_by or tablename_to_measure_by
  should be specified.
  Assumes that df_to_measure_by or tablename_to_measure_by's table
  is already grouped by the columns cols_to_group_by.
  """
  assert isinstance(cols_to_group_by, list)
  for col in cols_to_group_by:
    assert isinstance(col, str)
  assert isinstance(col_to_measure_by, str)
  assert isinstance(topnnum, int)
  assert topnnum > 0
  assert (df is not None) or df_table_name is not None
  assert (df is None) or df_table_name is None
  # assert df_to_measure_by == None or tablename_to_measure_by == None
  # assert (df_to_measure_by != None) or (tablename_to_measure_by != None)
  if df_to_measure_by is None and tablename_to_measure_by is None:
    df_to_measure_by = df if df is not None else read_table(cur, con, df_table_name)
  elif tablename_to_measure_by != None:
    assert isinstance(tablename_to_measure_by, str)
    #read from database if specified
    df_to_measure_by = read_table(cur, con, tablename_to_measure_by)
  if df is None:
    df = read_table(cur, con, df_table_name)
  #assert cols to group by are both in df and df to measure by
  cols_in_df_to_measure_by_set = set(list(df_to_measure_by.columns))
  cols_in_df_set = set(list(df.columns))
  for col in cols_to_group_by:
    assert col in cols_in_df_to_measure_by_set
    assert col in cols_in_df_set
  cols_to_select_by = cols_to_group_by + [col_to_measure_by]
  cols_to_measure_by_df = df_to_measure_by[cols_to_select_by].sort_values(by=col_to_measure_by, ascending=ascending)
  rows_to_measure_by_df = cols_to_measure_by_df.head(topnnum)[cols_to_group_by]
  cols_filtered_res = join_dfs(df, rows_to_measure_by_df, list_cols_to_join_on=cols_to_group_by)
  return cols_filtered_res