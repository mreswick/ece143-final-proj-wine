import pandas as pd
from wine_stat import freq
import seaborn as sb
import matplotlib.pyplot as plt

"""Module for visualizing data.
""" 

def plot_bar_chart(
  cur,
  con,
  table_name,
  col_of_labels,
  col_to_plot,
  y_axis_description,
  p_title,
  limit=None,
  hue=None,
  orient='v',
  figsize=[14, 6],
  bar_limits=None,
  label_bars=False,
  **kwargs #any extra args to seaborn.barplot(...)
):
  """Plots a bar chart of the given columns with the 
  given labels from the given table in the database,
  with the bars descending going from 
  left to right.
  Param:
    @limit: the number of topmost rows to select/graph,
    in descending order (ie the limit greatest get plotted)
  """
  assert isinstance(col_of_labels, str)
  assert isinstance(col_to_plot, str)
  assert isinstance(limit, int) or limit == None
  if isinstance(limit, int):
    assert limit > 0
  if bar_limits != None:
    assert isinstance(bar_limits, tuple)
    assert len(bar_limits) == 2
    for i in bar_limits:
      assert isinstance(i, int) or isinstance(i, float)

  #construct query string to select column of labels
  #and columns to plot. Gets the columns as having
  #the same name as in the table.
  plt.figure(figsize=figsize)
  hue_query_str = ""
  if hue != None:
    hue_query_str = f', {hue}'
  pd_table = pd.read_sql(f'SELECT {col_of_labels}, {col_to_plot}{hue_query_str}  FROM {table_name}', con)
  pd_table = pd_table.sort_values(by=col_to_plot,ascending = False)
  if limit:
    pd_table = pd_table.head(limit)
  #swap columns if plotting horizontal bar chart
  x = col_of_labels if orient != 'h' else col_to_plot
  y = col_to_plot if orient != 'h' else col_of_labels

  #palette used if graphing countries
  if x == 'country' or y == 'country':
    opac = 0.8 # opacity
    x_data = pd_table[x]
    y_data = pd_table[y]
    palette = []
    if orient=='v':
      col_data = x_data
      palette = [
            (0.1, 0.1, 0.8, opac) if xi == 'US'
        else (0.8, 0.5, 0.8, opac) if xi == 'Switzerland'
        else (0.8, 0.4, 0.0, opac) if xi == 'France'
        else (0.0, 0.9, 0.0, opac) if xi == 'Italy'
        else (0.9, 0.0, 0.0, opac) if xi == 'Spain'
        else (0.7, 0.0, 0.8, opac) if xi == 'Portugal'
        else (0.5, 0.2, 0.1, opac) if xi == 'Germany'
        else (0.5, 0.5, 0.8, opac) if xi == 'Canada'
        else (0.1, 0.4, 0.5, opac) if xi == 'Hungary'
        else (0.35, 0.4, 0.1, opac) if xi == 'Lebanon'
        else (0.5, 0.8, 0.9, opac) if xi == 'England'
        else (0.4, 0.2, 0.3, opac) if xi == 'India'
        else (0.8, 0.8, 0.8, opac) for xi in col_data
      ]
    else:
      col_data = y_data
      palette = [
            (0.1, 0.1, 0.8, opac) if xi == 'US'
        else (0.8, 0.5, 0.8, opac) if xi == 'Switzerland'
        else (0.8, 0.4, 0.0, opac) if xi == 'France'
        else (0.0, 0.9, 0.0, opac) if xi == 'Italy'
        else (0.9, 0.0, 0.0, opac) if xi == 'Spain'
        else (0.7, 0.0, 0.8, opac) if xi == 'Portugal'
        else (0.5, 0.2, 0.1, opac) if xi == 'Germany'
        else (0.5, 0.5, 0.8, opac) if xi == 'Canada'
        else (0.1, 0.4, 0.5, opac) if xi == 'Hungary'
        else (0.35, 0.4, 0.1, opac) if xi == 'Lebanon'
        else (0.5, 0.8, 0.9, opac) if xi == 'England'
        else (0.4, 0.2, 0.3, opac) if xi == 'India'
        else (0.8, 0.8, 0.8, opac) for xi in col_data
      ]
    bplot = sb.barplot(data=pd_table, x=x, y=y, palette=palette, **kwargs)
  else:
    bplot = sb.barplot(data=pd_table, x=x, y=y, **kwargs)
  
  #label bars of bar plots if specified
  if label_bars:
    #make room for labels
    if orient != 'h':
      bplot.margins(y=0.1)
    else:
      bplot.margins(x=0.1)
    #plot labels
    for bars in bplot.containers:
      bplot.bar_label(bars, padding=2.2)

  if orient != 'h':
    bplot.set_xticklabels(labels = pd_table[col_of_labels],rotation = 75)
    bplot.set_ylabel(y_axis_description)
    if bar_limits != None:
      # set y-ais limits
      plt.ylim(*bar_limits)
  else:
    bplot.set_xlabel(y_axis_description)
    if bar_limits != None:
      # set x-axis limits
      plt.xlim(*bar_limits)
  plt.title(p_title)
  plt.tight_layout()

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
  #plot showing percentages of the data
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
