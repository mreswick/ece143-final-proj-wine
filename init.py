import sqlite3 as sl
import pandas as pd
from wine_stat import freq # named it wine_stat so that it doesn't override python's stat package

#constants
WINE_INIT_DB_NAME = 'wine_init.db' # needs to end in .db
WINE_INIT_PATH_TO_DB = 'database/' + WINE_INIT_DB_NAME
WINE_INIT_TABLE_NAME = 'wine_init'
WINE_DATA_FILE = 'winemag-data-130k-v2.csv'
WINE_DATA_PATH = 'data/' + WINE_DATA_FILE

#helpers
def get_db(cur=None, con=None):
  #database vars
  con = con if con else sl.connect(WINE_INIT_PATH_TO_DB)
  cur = cur if cur else con.cursor()
  return (con, cur)

#testing:
def test_num_rows(cur):
  """Test number of rows in initial wine database.
  Params:
    @cur: cursor to initial wine database
  """
  #number of rows expected in initial wine table (from .csv file)
  NUM_TOTAL_EXPECTED_ROWS = 129971
  #get number of current rows
  num_wine_rows_obj = cur.execute("SELECT COUNT(*) FROM " + WINE_INIT_TABLE_NAME)
  num_wine_rows = num_wine_rows_obj.fetchall()[0][0]
  #test that both are equal
  assert NUM_TOTAL_EXPECTED_ROWS == num_wine_rows 

def init_wine_db(cur=None, con=None, testing=True):
  """Read csv file into database table.
  """ 
  #database vars
  con, cur = get_db(cur, con)
  #data
  wine_data = pd.read_csv(WINE_DATA_PATH)
  wine_data.to_sql(WINE_INIT_TABLE_NAME, con, if_exists='replace')
  #if testing, run some tests to ensure db is loaded correctly
  if(testing):
    test_num_rows(cur)

def set_freq_tables(cur=None, con=None, testing=True):
  """Create frequency tables for counts in various
  columns.
  """
  #database vars
  con, cur = get_db(cur, con)
  #set frequency of countries
  freq.set_db_freq_country(cur, con, WINE_INIT_TABLE_NAME, testing=testing)
  #set frequency of desginations
  freq.set_db_freq_desig(cur, con, WINE_INIT_TABLE_NAME, testing=testing)
  #set frequency of points
  freq.set_db_freq_points(cur, con, WINE_INIT_TABLE_NAME, testing=testing)
  #set frequency of provinces
  freq.set_db_freq_province(cur, con, WINE_INIT_TABLE_NAME, testing=testing)
  #set frequency of region1
  freq.set_db_freq_region1(cur, con, WINE_INIT_TABLE_NAME, testing=testing)
  #set frequency of region2
  freq.set_db_freq_region2(cur, con, WINE_INIT_TABLE_NAME, testing=testing)
  #set frequency of taster_name
  freq.set_db_freq_taster(cur, con, WINE_INIT_TABLE_NAME, testing=testing)
  #set frequency of taster_twitter_handle
  freq.set_db_freq_taster_twtr(cur, con, WINE_INIT_TABLE_NAME, testing=testing)
  #set frequency of wine 'variety'
  freq.set_db_freq_variety(cur, con, WINE_INIT_TABLE_NAME, testing=testing)
  #set frequency of wine 'winery'
  freq.set_db_freq_winery(cur, con, WINE_INIT_TABLE_NAME, testing=testing)

if __name__ == "__main__":
  init_wine_db()
  set_freq_tables()