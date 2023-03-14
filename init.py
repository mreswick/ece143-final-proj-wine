import sqlite3 as sl
import matplotlib
import pandas as pd
from wine_stat import freq, vis, common_stat # named it wine_stat so that it doesn't override python's stat package
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

def plot_price_grouping_bar_charts(cur, con):
  """Displays bar charts for various hard-coded column names
  given below. These are obtained by grouping by one or more
  other columns."""
  #plot bar chart for average price grouped by country
  common_stat.get_basic_stats_of_col1_grouped_by_cols(cur, con, 
  'price', ['country'], db_constants.WINE_INIT_TABLE_NAME)
  vis.plot_bar_chart(
    cur, 
    con, 
    'price_basic_stats_grouped_by_country',
    'country',
    'mean',
    y_axis_description='Mean price in $',
    p_title='Average Price of Wine By Country'
  )
  #plot bar chart for average price grouped by variety
  common_stat.get_basic_stats_of_col1_grouped_by_cols(cur, con, 
  'price', ['variety'], db_constants.WINE_INIT_TABLE_NAME)
  vis.plot_bar_chart(
    cur, 
    con, 
    'price_basic_stats_grouped_by_variety',
    'variety',
    'mean',
    limit=40,
    y_axis_description='Mean price in $',
    p_title='Average Price of Wine By Variety'
  )
  #plot bar chart for average points grouped by variety
  common_stat.get_basic_stats_of_col1_grouped_by_cols(cur, con, 
  'points', ['variety'], db_constants.WINE_INIT_TABLE_NAME)
  vis.plot_bar_chart(
    cur, 
    con, 
    'points_basic_stats_grouped_by_variety',
    'variety',
    'mean',
    limit=40,
    y_axis_description='Mean points awarded',
    p_title='Average Points of Wine By Variety'
  )
  #plot bar chart for average price grouped by region_1
  common_stat.get_basic_stats_of_col1_grouped_by_cols(cur, con, 
    'price', ['region_1'], db_constants.WINE_INIT_TABLE_NAME)
  vis.plot_bar_chart(
    cur, 
    con, 
    'price_basic_stats_grouped_by_region_1',
    'region_1',
    'mean',
    limit=40,
    y_axis_description='Mean price in $',
    p_title='Average Price of Wine By Region1'
  )
  #plot bar chart for average price grouped by province
  common_stat.get_basic_stats_of_col1_grouped_by_cols(cur, con, 
    'price', ['province'], db_constants.WINE_INIT_TABLE_NAME)
  vis.plot_bar_chart(
    cur, 
    con, 
    'price_basic_stats_grouped_by_province',
    'province',
    'mean',
    limit=40,
    y_axis_description='Mean price in $',
    p_title='Average Price of Wine By Province'
  )
  #plot bar chart for average price grouped by country and province
  # common_stat.get_basic_stats_of_col1_grouped_by_cols(cur, con, 
  # 'price', ['country', 'province'], db_constants.WINE_INIT_TABLE_NAME)
  # vis.plot_bar_chart(
  #   cur, 
  #   con, 
  #   'price_basic_stats_grouped_by_country_province',
  #   'country',
  #   'mean',
  #   limit=40,
  #   y_axis_description='Mean price in $',
  #   p_title='Average Price of Wine By Country-Province',
  #   hue='province'
  # )
  common_stat.get_basic_stats_of_col1_grouped_by_cols(cur, con, 
  'price', ['country', 'province', 'region_1'], db_constants.WINE_INIT_TABLE_NAME)
  # vis.plot_bar_chart(
  #   cur, 
  #   con, 
  #   'price_basic_stats_grouped_by_country_province',
  #   'country',
  #   'mean',
  #   limit=40,
  #   y_axis_description='Mean price in $',
  #   p_title='Average Price of Wine By Country-Province',
  #   hue='province'
  # )
  testpd = pd.read_sql('SELECT * FROM price_basic_stats_grouped_by_country_province_region_1', con)
  # t = testpd.groupby('country').sort_values(by='country', ascending=False).head(5)
  # print(t)
  # print(t[t['country'] == 'Argentina'])
  t = testpd.groupby(['country', 'province', 'region_1']).head(5)
  print(t)



# if __name__ == "__main__":
def init():
  con, cur = get_db(cur=None, con=None) #don't yet have connection to database
  #initialize database with overall wine data table with 
  data_cleaning.init_wine_table_with_null_cleaning(cur, con)

  print(" --> testing: ")
  #plot_price_grouping_bar_charts(cur, con)
  print("After having TEST displayed abrchart.")

  out_table = common_stat.get_basic_stats_of_col1_grouped_by_cols(
    cur,
    con,
    'price',
    cols_to_group_by=['country', 'province', 'region_1']
  )
  common_stat.get_table_recurs_limit_grouped_by(
    cur,
    con,
    input_table_name=out_table,
    col_of_scores='mean',
    cols_to_group_by=['country', 'province', 'region_1'],
    col_limits=[3, 2, 3],
    keep_duplicates=[True, True, True],
    sort_by_ascending=False
  )
  res_table_name = common_stat.get_table_recurs_limit_grouped_by(
    cur,
    con,
    input_table_name=out_table,
    col_of_scores='mean',
    cols_to_group_by=['country', 'province', 'region_1'],
    col_limits=[3, 2, 2],
    keep_duplicates=[True, False, False],
    sort_by_ascending=False
  )
  cols_to_group_by = ['country', 'province', 'region_1']
  print("After having called get_table_recurs.")
  res_name = common_stat.concat_str_columns(
    cur,
    con,
    res_table_name,
    cols_to_group_by
  )
  print("res_name: ", res_name)
  return cur, con


  # def get_table_recurs_limit_grouped_by(
  # cur,
  # con,
  # input_table_name,
  # col_of_scores,
  # cols_to_group_by,
  # col_limits,
  # keep_duplicates,
  # sort_by_ascending=False,
  # res_table_suffix=""):



  # #set and visualize via pie charts frequency info of columns in wine dataset
  # set_freq_tables(cur, con, testing=False)
  # # get top n_i tables for n_i = 5, 10, 20
  # get_top_ns_tables(cur=cur, con=con, n_list=[5, 10, 20])
  # # plot pie charts:
  # # - NOTE TO FIX OR BE AWARE OF: this just-below function call may use up too much memory 
  # #   if len(n_list) for n_list to get_top_ns_tables just above is too long (as will open 
  # #   up a pie chart for each top n table)
  # vis.plot_pie_charts_all_top_n_freq_tables(cur, con, freq.glbl_top_n_freq_schemas)
