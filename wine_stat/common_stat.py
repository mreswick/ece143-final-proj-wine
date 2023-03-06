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