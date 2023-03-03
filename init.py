import sqlite3 as sl
import pandas as pd

#constants
WINE_INIT_DB_NAME = 'wine_init.db' # needs to end in .db
WINE_INIT_PATH_TO_DB = 'database/' + WINE_INIT_DB_NAME
WINE_INIT_TABLE_NAME = 'wine_init'
WINE_DATA_FILE = 'winemag-data-130k-v2.csv'
WINE_DATA_PATH = 'data/' + WINE_DATA_FILE

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
  con = con if con else sl.connect(WINE_INIT_PATH_TO_DB)
  cur = cur if cur else con.cursor()
  #data
  wine_data = pd.read_csv(WINE_DATA_PATH)
  wine_data.to_sql(WINE_INIT_TABLE_NAME, con, if_exists='replace')
  #if testing, run some tests to ensure db is loaded correctly
  if(testing):
    test_num_rows(cur)
  
  
if __name__ == "__main__":
  init_wine_db()