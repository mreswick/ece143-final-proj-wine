import pandas as pd
from wine_stat import freq

"""Module for visualizing data.
""" 

def plot_pie_chart(
  cur,
  con,
  freq_table_to_plot,
  col_of_labels,
  col_of_counts
):
  """Plots a pie chart of the given table in the database.
  Param:
    @cur, con: connection vars for database
    @freq_table_to_plot: name of table to grab as dataframe to plot
    @col_of_labels: the column whose labels have been counted, and whose
    values/labels should be used as labels of the wedges in the pie chart
    @col_of_counts: the counts are being plotted by the pie chart
  """
  assert isinstance(freq_table_to_plot, str)
  plot_query_str = f'SELECT {col_of_labels}, {col_of_counts} FROM {freq_table_to_plot}'
  plot_df = pd.read_sql(plot_query_str, con)
  plot_df = plot_df.set_index(col_of_labels)
  #plots showing percentages of the data
  # NOTE TO-DO: some of the pie charts have the `y` label argument here appear  on top of their 
  # wedge labels; this leads to a messy pie chart. FIX THIS for those pie charts we need (if so)
  # that have this problem.
  pie_plot = plot_df.plot.pie(y=col_of_counts, figsize=(7,7), legend=True, autopct='%1.1f%%', title=col_of_counts, shadow=True, labeldistance =1.05, pctdistance=0.78)
  return pie_plot

def plot_pie_charts_for_freq_tables(
  cur, 
  con, 
  dict_top_n_freq_tables
):
  """Plots pie charts for the dictionary of
  table names to table schema (ie table columns here) as list. 
  Currently labels are expected to be in the first entry of this list
  for each table, and the frequency/counts in the second.
  Param:
    @cur, con: dictionary connection vars to init wine database
    @dict_top_n_freq_tables: has keys be table names, values be a
    list of its column names, where first must be the labels for the
    pie chart for it, and second the counts of it. 
      Ex:
        {
          freq_country_top_5: [country, freq_country],
          freq_variety_top_10: [variety, freq_variety]
        }
        The table names and column names do not matter,
        and n (n_i) can be a different value for each (for
        the number of top_n rows that were gotten to form that
        table, presumambly.)
  """
  assert isinstance(dict_top_n_freq_tables, dict)
  #plot pie chart for each table
  for (table_name, table_schema) in dict_top_n_freq_tables.items():
    col_of_labels_name, col_of_counts_name = table_schema[0], table_schema[1]
    #plot pie chart for table
    plot_pie_chart(cur, con, table_name, col_of_labels_name, col_of_counts_name)  

#if using default argument for dictionary, needs to be called
#after top-n tables are created (as default value for dictionary
#will be empty until they are).
def plot_pie_charts_all_top_n_freq_tables(
  cur,
  con,
  dict_freq_tables=freq.glbl_top_n_freq_schemas
):
  """Plots pie charts for the dictionary of
  top n (n_i) size to the dictionary of table names to 
  that of its table schema (ie table columns here) as list. 
  Currently labels are expected to be in the first entry of this list
  for each table, and the frequency/counts in the second.
  Param:
    @cur, con: dictionary connection vars to init wine database
    @dict_freq_tables: has keys be top n (n_i) sizes/values,
    then further keys as table names, vand finally alues be a
    list of that table's column names, where first must be the labels for the
    pie chart for it, and second the counts of it. 
      Ex:
        dict_freq_schemas = {
          5: {
            freq_country_top_5: [country, freq_country],
            freq_designation_top_5:[desgination, freq_designation]
          }
          20: {
            freq_variety_top_20: [variety, freq_variety]
          }
      }
  Importantly, if using the default value here, then those top_n tables
  in that dictionary (see freq.py file for more descriptoin) must exist
  before this function is called (otherwise this default value dicitonary
  will be empty).
  """
  for (_, dict_top_n_freq_tables) in dict_freq_tables.items():
    #plot the pie charts for those top_n tables with n=n_i, of ie, for those n_i
    plot_pie_charts_for_freq_tables(cur, con, dict_top_n_freq_tables)
