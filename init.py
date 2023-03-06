import sqlite3 as sl
import pandas as pd
from wine_stat import freq # named it wine_stat so that it doesn't override python's stat package
from wine_stat import vis  # named it wine_start so that it doesn't override python's stat package
from data_cleaning import data_cleaning
from database import db_constants

#constants
WINE_INIT_DB_NAME = db_constants.WINE_INIT_DB_NAME # needs to end in .db
WINE_INIT_PATH_TO_DB = 'database/' + WINE_INIT_DB_NAME
WINE_INIT_TABLE_NAME = db_constants.WINE_INIT_TABLE_NAME
WINE_DATA_FILE = db_constants.WINE_DATA_FILE
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

def get_top_ns_tables_for_n(cur=None, con=None, n=5):
  """Create top n tables from frequencies for
  various columns.
  Currently uses global dictionary of frequency table names and their schemas
  from wine_stat/freq.py, so this function needs to be called
  after that dictionary is created (currently after set_freq_tables
  of this file is called).
  """
  #database vars
  con, cur = get_db(cur, con)
  freq.get_top_n_rows_of_each_freq_table(cur, con, n)

def get_top_ns_tables(cur=None, con=None, n_list=[5]):
  """Takes a list of n's (n_i's), n_list, and creates a top_n table for each
  frequency table for each of those n_i. This function should thus be called
  after the frequency tables are created (currently after set_freq_tables
  of this file is called).
  """
  assert isinstance(n_list, list) or isinstance(n_list, int)
  #make as list if just a single entry passed
  n_list = [n_list] if isinstance(n_list, int) else n_list
  for n_i in n_list:
    get_top_ns_tables_for_n(cur, con, n_i)


if __name__ == "__main__":
  con, cur = get_db(cur=None, con=None) #don't yet have connection to database
  #initialize database with overall wine data table with 
  data_cleaning.init_wine_table_with_null_cleaning(cur, con)
  
  #set and visualize via pie charts frequency info of columns in wine dataset
  set_freq_tables(cur, con, testing=False)
  # get top n_i tables for n_i = 5, 10, 20
  get_top_ns_tables(cur=cur, con=con, n_list=[5, 10, 20])
  # plot pie charts:
  # - NOTE TO FIX OR BE AWARE OF: this just-below function call may use up too much memory 
  #   if len(n_list) for n_list to get_top_ns_tables just above is too long (as will open 
  #   up a pie chart for each top n table)
  vis.plot_pie_charts_all_top_n_freq_tables(cur, con, freq.glbl_top_n_freq_schemas)
