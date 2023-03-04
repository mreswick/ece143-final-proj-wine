import pandas as pd

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
    @col_of_counts: the counts are being plotted by the pie chart
  """
  assert isinstance(freq_table_to_plot, str)
  plot_query_str = f'SELECT * FROM {freq_table_to_plot}'
  plot_df = pd.read_sql(plot_query_str, con)
  plot_df = plot_df.set_index(col_of_labels)
  pie_plot = plot_df.plot.pie(y=col_of_counts, figsize=(5,5), legend=True)