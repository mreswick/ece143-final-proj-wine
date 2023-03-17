import pandas as pd
import os # for path to data, see below in count_nulls where used
from wine_stat import common_stat, null_info
from database import db_constants

#constants
WINE_DATA_PATH = f'../data/winemag-data-130k-v2_new.csv' # already had cleaning done on it #f'../data/{db_constants.WINE_DATA_FILE}' 
TEMP_WINE_DATA_TABLE = 'temp_wine_data' #wine data before cleaning;
                                        #is deleted after cleaning
WINE_INIT_TABLE_NAME = db_constants.WINE_INIT_TABLE_NAME

def drop_null_entries_from_cols(cur, con, table_name, col_names):
  """Drops all null entries from columns in col_names from the
  table with name table_name, and overwrites/replaces that table
  with the resultinig table. THIS WILL THUS OVERWRITE THE ORIGINAL
  TABLE, keep in mind.
  Param:
    @cur, con: database vars
    @table_name: table to remove (some) null entries from
    @col_names: the columns to remove null entries from. Columns
    not specified in col_names thus may have null entries still
    in them. This is a list of string column names.
  """
  assert isinstance(table_name, str)
  assert isinstance(col_names, list)
  for col_name in col_names:
    assert isinstance(col_name, str)
  assert len(col_names) > 0
  table_data = pd.read_sql(f'SELECT * FROM {table_name}', con)
  table_data.set_index('index')
  #query string to drop all nulls in only the specified columns
  query_str = f'SELECT * FROM {table_name} WHERE {col_names[0]} IS NOT NULL'
  for col_name in col_names[1:]:
    query_str += f' AND {col_name} IS NOT NULL'
  table_to_replace_with_data = pd.read_sql(query_str, con)
  #replace table with new table
  # I don't write the index here (index=False), as table reading from
  # presumambly already has it
  table_to_replace_with_data.to_sql(table_name, con, index=False, if_exists='replace')


def init_wine_table_with_null_cleaning(cur, con, new_table_name=WINE_INIT_TABLE_NAME):
  """Initializes and writes to the database the overall wine table with null
  values cleaned out. Note that NOT ALL NULL ENTRIES ARE REMOVED here: this is
  because some columns have lots of nulls (like >20%), and so those are 
  still kept here.
  Param:
    @cur, con: database vars
    @new_table_name: the name to write the initial, overall wine table name
    with all the data (except for cleaned out nulls) to the database with
  """
  #data
  # os.path used here as I had trouble using ".." in relative path on Windows
  wine_data = pd.read_csv(os.path.join(os.path.dirname(__file__), WINE_DATA_PATH))
  # drop the "Unamed: 0" second column, as pandas has index column instead
  wine_data = wine_data.drop('Unnamed: 0', axis=1)
  wine_data.to_sql(TEMP_WINE_DATA_TABLE, con, if_exists='replace')

  # print summary info about nulls in initital overall wine table
  init_total_num_rows = common_stat.get_num_rows_in_table(cur, con, TEMP_WINE_DATA_TABLE)
  null_info.print_null_info_for_table(cur, con, TEMP_WINE_DATA_TABLE)
  """As a result of the printed results above, print the following
  to inform the user about which null entries we decide to drop.
  (Currently only drop those that makeup less than 10% of our overall data).
  """
  print_str_null_drop_info = """Based on the null info for out initial \
dataset, we will drop rows with nulls for the following columns:
     - country, price, province, variety
    We drop rows with nulls from these columns as these together only \
makeup less than 10% of our overall data. This is because each \
has the following percent null:
         - country: 0.05%
         - price: 6.92%
         - province: 0.05%
         - variety: 0.00% (has 1 null entry)
    The other columns with nulls makeup a greater percentage of the \
number of rows in our data, and so we keep nulls in those. These \
include:
          - designation (28.83% null)
          - region_1 (16.35% null)
          - region_2 (61.14% null)
          - taster_name (20.19% null)
          - taster_twitter_handle (24.02% null)"""
  print(print_str_null_drop_info)
  col_names_to_drop_null_entries_from = ['country', 'price', 'province', 'variety']
  drop_null_entries_from_cols(cur, con, 
    TEMP_WINE_DATA_TABLE, col_names_to_drop_null_entries_from)
  # now print summary again with new table with cleaned nulls
  new_print_str_null_drop_info = """As a result of having dropped the before \
mentioned null entries in specific columns (country, price, province, and \
variety), the number of rows and null info for our dataset is now as \
follows: """
  print(new_print_str_null_drop_info)
  null_info.print_null_info_for_table(cur, con, TEMP_WINE_DATA_TABLE)

  #write table with cleaned nulls to database as our wine init overall table
  # (this is done below by copying it over from the temporary table
  # with these cleaned nulls)
  new_wine_data = pd.read_sql(f'SELECT * FROM {TEMP_WINE_DATA_TABLE}', con)
  #  Don't use (another) index here as presume it already exists as column in
  #  the temporary table; ie index=False here.
  new_wine_data.to_sql(new_table_name, con, index=False, if_exists='replace')

  #print some info about the change in table size as a result
  #of having dropped (some) nulls
  # number of rows after having dropped nulls in the above columns
  res_total_num_rows = common_stat.get_num_rows_in_table(cur, con, TEMP_WINE_DATA_TABLE)
  # number of rows dropped as a result of having cleaned out (some) nulls
  num_rows_diff = init_total_num_rows - res_total_num_rows
  # percentage of rows retained (rounded to two decimal places)
  res_data_size_percentage = round(100 * (res_total_num_rows/init_total_num_rows),2)
  percent_diff = round(100 - res_data_size_percentage, 2)
  # print summary info
  print('and thus as null-cleaning summary: ')
  print(f' - The number of rows dropped as a result of cleaning out the before-mentioned nulls is: {num_rows_diff} rows.')
  print(f' - This means that of the original rows, null cleaning has decreased the size (ie number of rows) of our dataset by: {percent_diff}%.')
  print(f' - Our resulting data size is thus {res_data_size_percentage}% the size (ie number of rows) of the original dataset.')

  #print confirmation that table is initialized with (some) cleaned/removed nulls
  print(f'-- Confirmation: newly-created table with name "{new_table_name}" now with cleaned nulls written to database with that name. --')
  print("")