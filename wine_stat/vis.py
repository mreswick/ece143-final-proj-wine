import pandas as pd
from wine_stat import freq
import seaborn as sns
import matplotlib.pyplot as plt
import colorcet as cc
from database import db_constants
import sqlite3 as sl
import plotly.express as px
from pyecharts.charts import Map
from pyecharts import options as opts


"""Module for visualizing data.
""" 

def draw_fig(df, ct, title_ = 'Top Flavors', save=True, topk=5, max_=1):
  """
  Draw barplots for top descriptive words and assign each word an unique color.
  Param:
      @df: the dataframe
      @ct: counts_table produced from counts_table()
      @title_: the title of the plot
      @save: whether to save the plot
      @topk: plots topk number of words for each points interval
      @max_: xlim max range of all subplots
  """
  uniques = set()
  def fun(x):
      for i in x:
          uniques.add(i[0])
      return True
  ct.apply(lambda x: fun(x))
  
  custom_palette = {i:j for i,j in zip(uniques, sns.color_palette(cc.glasbey, n_colors=len(uniques))) }
  #print(custom_palette)
  
  fig, axs = plt.subplots(1,len(ct),figsize = (3*len(ct),5))
  first = True
  for ax, g_id in zip(axs,ct.index):
      df_tmp = pd.DataFrame(ct[g_id], columns = ['name','value'])[:topk]
      df_tmp['value'] = df_tmp['value']/(len(df)//len(ct))
      plot_ = sns.barplot(data=df_tmp, y='name', x='value', ax=ax, orient='h', palette=custom_palette)
      plot_.set(title=str(g_id))
      plot_.set(xlabel=None)
      plot_.set(ylabel=None)
      plot_.set(xlim=(0, max_))
      if first:
          first = False

      else:
          #plot_.set(yticklabels=[])
          first = first

  fig.suptitle(title_, fontsize=14)
  #fig.text(0.5, 0.0, 'common X', ha='center')
  fig.text(0.5, 0.0, 'Frequency', ha='center')
  plt.tight_layout()
  
  if save:
      plt.savefig(f'./{title_}.jpg',dpi=300)
  
  plt.show()    

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
    bplot = sns.barplot(data=pd_table, x=x, y=y, palette=palette, **kwargs)
  else:
    bplot = sns.barplot(data=pd_table, x=x, y=y, **kwargs)
  
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

# ----------------------------------------------- Overview ----------------------
def readin_db_to_df(path='database/'):
  """Reads in database to dataframe here.""" 
  # read in database
  WINE_INIT_DB_NAME = db_constants.WINE_INIT_DB_NAME
  WINE_INIT_PATH_TO_DB = path + WINE_INIT_DB_NAME
  con = sl.connect(WINE_INIT_PATH_TO_DB)
  sql_cmd = "SELECT * FROM wine_init"
  df = pd.read_sql_query(sql_cmd, con, index_col = 'index')
  con.close()
  return df

def sunBurstChart_winecount(df=readin_db_to_df(), color='Bugn'):
  """Creates a sun-burst chart of the wine count.""" 
  # wine_count under country -> province -> region
  # pick 3 columns and do freq count
  df_CnPR = df[['country', 'province','region_1']].value_counts()
  # organize the dataframe
  df_CnPR = pd.DataFrame(df_CnPR).reset_index().rename(columns = {0 : 'wine_count'})
  # drawing plot:
  fig=px.sunburst(df_CnPR,path=['country','province','region_1'],values='wine_count',color='wine_count',color_continuous_scale=color)
  fig.update_traces(textinfo='label + percent parent + value')

def price_point_all(df=readin_db_to_df()):
  """Plot relationship between price and points.""" 
  title = "Relationship between price and point"
  plt.figure(figsize = [14,6])
  ax = sns.regplot(x = df["price"], y="points", data=df, logx=True, truncate=True)
  plt.title(title,fontsize='x-large')
  plt.tight_layout()
  # plt.savefig(f'./{title}.jpg',dpi=300)

def price_point_province(df=readin_db_to_df(), n=3):
  """Plot relationship between price and points for provinces."""
  topProvince = df['province'].value_counts(normalize = True).head(n).index.tolist()
  df_plimit = df[df['price']<=100]
  count = 1
  for p in topProvince:
    title = f'Hex grid of price and point in {p}'
    plt.figure(figsize = [14,6])
    ax = sns.jointplot(x='price', y='points', data=df_plimit.loc[df_plimit['province'].isin([p])], kind='hex',gridsize=20)
    plt.suptitle(title)
#     plt.savefig(f'./0{count}-{title}.jpg',dpi=300)
    count += 1

def price_point_variety(df=readin_db_to_df(), n=3):
  """Plot price and point for wine varieties.""" 
  df_plimit = df[df['price']<=100]
  topVariety = df['variety'].value_counts(normalize = True).head(n).index.tolist()
  count = 1
  for v in topVariety:
      title = f'Hex grid of price and point in {v}'
      plt.figure(figsize = [14,6])
      ax = sns.jointplot(x='price', y='points', data=df_plimit.loc[df_plimit['variety'].isin([v])], kind='hex',gridsize=20)
      plt.suptitle(title)
  #     plt.savefig(f'./0{count}-{title}.jpg',dpi=300)
      count += 1

# ----------------------------------------Overview Map --------------------------
def count_Country(df=readin_db_to_df()):
  """Creates a world map of the count of number of reviews
  per country.""" 
  #get the data that can be read in 'echarts' (echarts.apache.org)
  df_map = df[df['price'] <= 1000]
  df_map.replace("US","United States",inplace=True)
  df_map.replace("England","United Kingdom",inplace=True)
  df_map.replace("Czech Republic","Czech Rep.",inplace=True)
  df_map.replace("Bosnia and Herzegovina","Bosnia and Herz.",inplace=True)

  country=list(df_map['country'].value_counts().index)
  counts=list(df_map['country'].value_counts().values)

  a="'name'"
  b="'value'"
  data=[country,counts]
  CountsPerCountry=[{eval(a):data[0][i],eval(b):data[1][i]} for i in range(len(country))]
  link_str = '''
  the chart can be displayed at the link below:
  https://echarts.apache.org/examples/en/editor.html?c=map-usa&code=LYTwwgFghgTgLgOgM4QPYHcAyqoBMCWAdgOYAUAlANwBQAJAsQKZykBKA8uwCoD6ACgEEuACQAEAalEByAPS4ocKDKhIkzGU1Qz0qGABtcCAFZJUhKQBpRAMwCuhAMZx8Z0aVtIoAKVOFyogG9qUVFQSFhECHxcRmw8IjIqYNFGB2h4JAQYRmJ8JDhGGABZKAAHUikAVQBlAUtRD29fKyCQkIE9FQBrKAAuQOS20T1Gazh-gFoARgBmKYtBtrhUUv6AJgBWBaGQ9Gi4CH6pjcWAX2224Sh0KHx8ftadkbHJqamABguh5dXRNYAOL5tPa4A79E5Dc6LKR8WyFZaiVj4ByoKQPRYhZ7jUQTADsADYgSEfutCRjRCCwX8zslTkkQitnK4ALwDb74OAjdE7UQFAAe2KkAHUiIxEYwAG74RjoURgVD2OBIURC3QGEGMSzkrH9KQwfDECBwKRnIHLVB6Zy_R7ffXEJgwXUcxjALU8lAYAAijE6IH6n3JcBgUEISA5LkIntswaZhH9CDWpsWUqQtigehK1u1o0FdqNbp2wCIRyJoSgfPBABY1vjK6WiKwQ0xuTzRCi9Lp-gBtclDKQAYjmM3xAE4NgXW9J-5WNriNgAjSsT1sD3GVvC4KbLnkDqDz3Ajxgj7c7AeMd7WGbWf4nvv96wP6zz6y3toD6yMc8j96vkLv-SMPiW6lne1iVviuCVjMv5TrguIzO8ay4jBu4bO8iH4iak4ALrklCPL8tiXZSMIBoQPUUjYOgUjYaWDjpg4tidPOXK8jAcJJt8qAWvOqAVmyOweug_RBnCpYyDIujSoQgoSvCyLpieOrSFiJ4ktIPwnh-CjRowLY8vIigAGrSsJgSiNkeDsIQeh-jY6ZqKI-Gttk-S6HpgTOe6UByQISAAJLAFAzaeXhnFtGo-qMEg3bkjaOyEFAwAecKorilKMpygqMlIK-cAgKUKVBaUr4wDgwAiexjAgWWvxVLUr4uqU0BhjFAmTsxPr6ZOohCZVHE9U5vZOTVhl9KIXYBFIiXJWi0iVIQzq4KI1SKAUuVWFIErpnCc0bNWtbnIE01JZquoAGLBo4mqbdtei7UcuIEpWR1TTNZ3SP5ii2RRd0PaIUyjlML0tCds26tUpS3OYt07R9-KzjMr1gx9MK6HAtjEIpsP3R9lb_HOyPvXNUiQPgIy_XDc2VpWgNE6dJMCDATAyUQUCU7jc0zHO-L0-D0gCB4Qb4OzOP_UhI7_HzqOC_kwZ6CLHPi2sI4g8dxO6gA4oUQWECASsfWsUxrO80skwAcplABajDpiGuAG3Nsy4lLoMa9I1QKgcogCNY-r0Y7Rwq0jbsM7q_lIMGPqB6I-MjmbWvZKkN3SH9eNAQn0hgCGeCi6nVPrPtmckfYWMwPrYsfcDGzFwAQkxZeK5XTu08XrCoLrTf55zQem6H_NVOxxBphX3f_R88f96jXDRl0jCj1tBeiN-xfa7ouR54vPeiP8atvWHHsdnJi2b2nc3_H36sH6TZUKF3W__biUzF0UjB8siqLN_0uKX_vA8LUtUQABpBIuAO4x1HC_C0YDtoxw2JPK-A9a7BgAF7kxjpWXExdMCMHnCGMwMcZg1yniTIouhUAOBRDHAExc-CFFsDHOmJDdSVC6MGUUjC94oxJtUQo8575nyDsXMAKDUgQHFKUBAjC1gvygA4RgYCT7SOEQVdiG0x5V2fswz6hACCnyXggv-qNqh7DgKI_Q9sY5YO0ZRWwfIXS8WjMQCBddUBIBPqIe2ohhCFBQVIr-fxi5M2SkogJMibHVCPlALoAil5aMQajMmiVGGnFwjyU4iw0lORoCEMI6REBqDgOwUosZSCMgjEkOklAgA
  '''
  print(link_str)

def point_mean_description_length(df=readin_db_to_df()):
  """Plots the points against the average description length
  per taster.""" 
  #get the correlation plot between Points and Mean Description Length and Mean Price group by tasters
  df_points=df[df['price'] <= 1000]
  df_points['description_length']=df_points['description'].str.len()

  #get the correlation plot between Points and Mean Description Lengthacross the whole dataset 
  sns.set_theme(style="darkgrid")
  g=sns.lmplot(data=df_PointsMeanLength,x="description_length",y="points",scatter_kws={"s": 50, "alpha": .5})
  g.set(xlim=(100,450))
  title_="Correlation between Points and Mean Description Length"
  plt.title(title_,fontsize='x-large')
  #plt.savefig(f'./{title_}.jpg',dpi=300)

  df_top5Taster = df_points[df_points["taster_name"].str.contains("Roger Voss|Michael Schachner|Kerin O’Keefe|Virginie Boone|Paul Gregutt")==True]
  for taster in ["Roger Voss","Michael Schachner","Kerin O’Keefe","Virginie Boone","Paul Gregutt"]:
      df_taster=df_top5Taster[df_top5Taster["taster_name"]==taster]
      
      df_PointsMeanLength = df_points.groupby("points").agg({'description_length':'mean'}).sort_values(by = 'description_length',ascending = False)
      df_PointsMeanLength=df_PointsMeanLength.reset_index()
      
      df_PointsMeanPrice=df_taster.groupby("points").agg({'price':'mean'}).sort_values(by = 'price',ascending = False)
      df_PointsMeanPrice=df_PointsMeanPrice.reset_index()
      
      g=sns.lmplot(data=df_PointsMeanLength,x="description_length",y="points",scatter_kws={"s": 50, "alpha": .5},logx=True)
      g.set(xlim=(100,450))
      title_="Mean Description Length Given Points - "+taster
      plt.title(title_,fontsize='x-large')
      #plt.savefig(f'./{title_}.jpg',dpi=300,bbox_inches='tight')
      
      g=sns.lmplot(data=df_PointsMeanPrice,x="price",y="points",scatter_kws={"s": 50, "alpha": .5,"color":"m"},line_kws={"linewidth": 3, "alpha": .5,"color":"m"},logx=True)
      g.set(xlim=(0,600))
      title_="Mean Price Given Points - "+taster
      plt.title(title_,fontsize='x-large')
      #plt.savefig(f'./{title_}.jpg',dpi=300,bbox_inches='tight')