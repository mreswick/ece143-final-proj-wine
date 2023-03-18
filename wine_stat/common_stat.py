import pandas as pd
from database import db_constants
from collections import defaultdict

"""A general-purpose statistics module,
and other basic operations (like number
of rows in table)."""

def counts(df_col):
    """
    Helper function for counts_table.
    Return counts for each word.
    Param:
        @df_col: The column of a df
    """
    cc = defaultdict(int)
    
    def fun(x):
        for i in x:
            cc[i] += 1
        return True
    df_col.progress_apply(lambda x: fun(x) )
    
    return sorted(cc.items(), key=lambda x:x[1], reverse=True)

def percent_range(col_name, df_, N = 5):
    """
    Helper function for counts_table
    Produce N intervals, where each interval contains same number of samples
    Param:
        @col_name: the col name of the df_
        @df_: the dataframe
        @N: the number of intervals
    """
    percent = [(i+1) * (1.0/N) for i in range(N-1)]
    x_percentile = df_[col_name].describe(percentiles=percent)
    
    if N %2 == 1:
        x_percentile = x_percentile.drop(index=['50%'])
    
    ranges = x_percentile[3:].values
    return ranges

def counts_table(df, col_name, common_filter=0):
    """
    Produce a counts table that for each points interval, it compute the word counts.
    Param:
        @df: the dataframe
        @col_name: should be "points"
        @common_filter: number of common words are filtered
    """
    df['points_range'] = pd.cut(df["points"], percent_range("points", df))
    
    common_words = set([i[0] for i in counts(df[col_name])[:common_filter] ])
    
    return df.groupby('points_range')[col_name].apply(lambda x: counts(x)).apply(lambda x: [i for i in x if i[0] not in common_words])

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

def get_num_unique_rows_in_column(cur, con, table_name, col_name):
  """Gets the number of non-null distinct/unique rows in the given
  column in that table; returns this as an integer. Null entries
  are thus ignored / not included in this count.
  Param:
   @cur, con: database vars
   @table_name: name of the table to use
   @col_name: column in table to se
  """
  COUNT_DISTINCT_COL_RSE_NAME = "COUNT_COL_DISTINCT"
  pd_table = pd.read_sql(f'SELECT COUNT(DISTINCT {col_name}) AS {COUNT_DISTINCT_COL_RSE_NAME} FROM {table_name}', con)
  total_distinct_count = pd_table[COUNT_DISTINCT_COL_RSE_NAME][0]
  return total_distinct_count 

def get_num_uniq_rows_for_each_col_in_table(cur, con, table_name):
  """Gets the number of non=null distinct/unique rows for
  each column in that table; returns this as a dictionary
  with the keys as column names and the values as these
  counts.
  Param:
    @cur, con: database vars
    @table_name: the name of the table to get the number
    of unique entries in each column for
  """ 
  pd_table = pd.read_sql(f'SELECT * FROM {table_name}', con)
  pd_table.set_index('index')
  col_names = list(pd_table.columns.values)
  dict_uniq_row_counts = {}
  for col_name in col_names:
    if col_name != 'index': # don't count values for index column
      dict_uniq_row_counts[col_name] = get_num_unique_rows_in_column(cur,
        con, table_name, col_name)
  return dict_uniq_row_counts

def concat_str_columns(cur, con, table_name,
  cols_to_concat):
  """Concatenates string columns to be hyphen-separated.
  Param:
    @table_name: table of database to concatenate
    columns with
    @cols_to_concat: list of string columns to concatenate,
    hyphen-separated. Resulting column name of this
    is the names of those columns also concatenated
    hyphen-separated. Concatenates in the order
    from left-to-right as items are specified to this
    list.
  return
    Returns the name of the table this function creates
    with the newly-created concatenated column.
  """
  assert isinstance(table_name, str)
  assert isinstance(cols_to_concat, list)
  assert len(cols_to_concat) > 0
  for col in cols_to_concat:
    assert isinstance(col, str)
  col_name_res = "strconcat-" + '-'.join(cols_to_concat)
  table_name_res = f'{table_name}-{col_name_res}'
  new_pd_table = pd.read_sql(f'SELECT * FROM {table_name}', con)
  new_pd_table[col_name_res] = ""
  first_el = True
  for col in cols_to_concat:
    hyphen_str = "" if first_el else "-"
    first_el = False
    new_pd_table[col_name_res] += hyphen_str + new_pd_table[col]
    #remove just-concatenated column from table
    new_pd_table = new_pd_table.drop(columns=[col])
  new_pd_table = new_pd_table.set_index('index') #restore index
  #write resulting table to database
  new_pd_table.to_sql(table_name_res, con, if_exists='replace')
  return table_name_res

def get_table_recurs_limit_grouped_by(
  cur,
  con,
  input_table_name,
  col_of_scores,
  cols_to_group_by,
  col_limits,
  keep_duplicates,
  sort_by_ascending=False,
  res_table_suffix=""):
  """Creates and returns the name to a table
  that is the result of "recursively"/iteratively
  limiting the number of children for each parent
  category/group label, kept in sorted order
  of the given column of scores.
  For the following description, this function
  treats the given input table as a tree from
  left to right (with root on far left, as
  an imaginary column before the first),
  where branches represent paths from left to
  right in this table, visiting an element in 
  a given column a maximum of one time for that
  given path, and with the same elements in 
  columns to its left as for each entry encountered
  when going to the right in the table. 
  Param:
    @cur, con: database vars
    @input_table_name: the name of the table
    in the database to perform the grouping on
    @col_of_scores: the name of the column
    in the table to use as the scores to group-by
    and sort 
    @cols_to_group_by: a list of the columns 
    (their column names) to group by.
    These can be visualized as a tree from
    left-to-right in the order that they 
    are specified, in terms of the resulting
    table that is created, in the same order
    (ie in the order they are specified
    in this list from first element to
    last element)
    @col_limits: a list of positive integers
    that each specify the number of children
    nodes (THESE MAY BE DUPLICATED UNLESS
    OTHERWISE SPECIFIED BY THE CORRESPONDING
    ENTRY OF keep_duplicates) of the corresponding 
    index, from left to right, that are to be kept
    in the given sorted order.  
    @keep_duplicates: a list that must be
    the same length as the list of columns
    to group by, and for each, determines
    whether duplicates are to be kept.
    If specified as False, then this causes
    the children limited by col_limits
    for each given parent node of the index just
    before to be distinct. If the corresponding
    entry in this list is true, then col_limits
    is effectively only specifying the top n
    entries to keep at that given child level
    for each parent, but if false, then is
    specifying the number of (distinct) children
    it may have. If duplicate children,
    then those that are higher up in the given
    sorted order are kept.
    @sort_by_ascending: is a boolean value
    for how to sort the column of scores. If
    true, then it is sorted ascending, otherwise
    descending. This affects which elements/nodes
    at each level of the table 
    @res_table_suffix: a suffix to add to the end
    of the table namem this function creates.
    This function thus automatically generates
    the resulting table name for the table it
    creates, optionally with this suffix
    appended to it if specified.
  Note that the input table should already have
  been grouped by the columns cols_to_group_by.
  This IS NOT done in this function, as it
  would most likely not make sense what the 
  resulting values in the column of scores
  should be if this grouping was not done
  already.
  
  Example: 
    If:
      - input_table_name='test',
      - col_of_scores='X'
      - cols_to_group_by=['A', 'B', 'C']
      - col_limits=[2, 2, 1]
      - keep_duplicates=[True, True, True]
      - sort_by_ascending=False
    and if the input table 'test' is as follows:
        A  B  C   X
      0  a  x  d  20
      1  a  x  e  30
      2  a  y  e  40
      3  b  z  d  50
      4  b  a  e  10
      5  b  z  e  20
      6  c  w  d  80
      7  c  x  d  50
      8  c  x  e   5
      9  c  z  e  45
    then the resulting table will then be:
        A  B  C   X
      6  c  w  d  80
      3  b  z  d  50
      7  c  x  d  50
    As intermediate steps to see this, firstly,
    col_limits[0]==2 specifies that only entries
    for the first column, 'A', that have the 
    top-two maxes in the table, should be kept.
    The intermediate result of this is:
        A  B  C   X
      6  c  w  d  80
      3  b  z  d  50
      7  c  x  d  50
      9  c  z  e  45
      5  b  z  e  20
      4  b  a  e  10
      8  c  x  e   5
    Notice that rows with A as 'a' have been dropped,
    as only the top-two (by sort_ascending=False) have
    been kept. Notice also that 'b' and 'c' can each
    appear here multiple times, as keep_duplicates[0]==True.
    (If it had been false, then only two rows would be left
    here: one with the top X value for c, and one
    with the top X value for b). 
    Next is col_limits[1]==2, which yields as an intermediate
    result:
        A  B  C   X
      6  c  w  d  80
      3  b  z  d  50
      7  c  x  d  50
      5  b  z  e  20
    For each parent element in column A, only the top two
    children (really here, for 'b', they are both 'z',
    as keep_duplicates[1]==False, and so are not really
    children in terms of being distinct) are kept.
    Next, due to col_limits[2]==1, for each grouping
    of elements for columns 'A' and 'B', we keep only
    those rows with 'C' for that grouping that has
    the topmost X for that groupining of 'A' and 'B'.
    This yields:
        A  B  C   X
      6  c  w  d  80
      3  b  z  d  50
      7  c  x  d  50
    as the final result. Notice that the row
      5  b  z  e  20
    has been dropped, as of C='d' or C='e' for
    the grouping for A B 'b' 'z', that for C='d'
    has a higher X value (50 vs. 20),
    (where higher is used by sort_ascending=False; 
    it would have been that with lower X value
    if sort_ascending=True).
    
  Ex #2: if we had the above example with everything the same
  except that:
      - keep_duplicates=[True, False, True]
    then, as noted in Ex #1 above, instead of the intermediate
    result:
        A  B  C   X
      6  c  w  d  80
      3  b  z  d  50
      7  c  x  d  50
      5  b  z  e  20
    we would instead have had the intermediate result of:
        A  B  C   X
      6  c  w  d  80
      3  b  z  d  50
      7  c  x  d  50
      4  b  a  e  10
    where the two bottom rows differ here because in the
    former, they both have 'b z', but because
    keep_duplicates[1]=False, duplicates are first
    eliminated before the top (by sort_ascending=False here)
    two entries here (by col_limits[1]==2) are retained.
    This causes the lower 'b z' row in this example to be
    eliminated, with the top two resulting that thus 
    have distinct B values, which here are for 'b z' 
    (with index 3) and 'b a' (with index 4).
  
  Ex #3: note that specifying keep_duplicates[0]=False is somewhat
  of a special general case in that it will cause the first column
  to be limited to col_limits[0] number of entries, thereby
  always restricting the entire resulting table to be that 
  same number of rows.
  """ 
  #assert types
  assert cur != None and con != None
  assert isinstance(input_table_name, str)
  assert isinstance(col_of_scores, str)
  assert isinstance(cols_to_group_by, list)
  assert len(cols_to_group_by) > 0
  for col in cols_to_group_by:
    assert isinstance(col, str)
  assert isinstance(col_limits, list)
  assert len(col_limits) == len(cols_to_group_by)
  for col_limit in col_limits:
    assert isinstance(col_limit, int)
    assert col_limit > 0
  assert isinstance(keep_duplicates, list)
  assert len(keep_duplicates)==len(cols_to_group_by)
  for keep_duplicate in keep_duplicates:
    assert isinstance(keep_duplicate, bool)
  assert isinstance(sort_by_ascending, bool)
  assert isinstance(res_table_suffix, str)
  #assert columns exist in input table
  df = pd.read_sql(f'SELECT * FROM {input_table_name}', con)
  set_of_col_names = set(list(df.columns))
  for col in cols_to_group_by:
    assert col in set_of_col_names
  assert col_of_scores in set_of_col_names

  #read table and process, in the form of
  # cols_to_group_by col_of_scores
  #from left to right. This will thus also
  #be the order of columns for the resulting table.
  # build query string
  query_str = f'SELECT {cols_to_group_by[0]}'
  for col_to_group_by in cols_to_group_by[1:]:
    query_str += f', {col_to_group_by}'
  query_str += f', {col_of_scores}'
  query_str += f' FROM {input_table_name}'
  # get table and process per parameters
  df = pd.read_sql(query_str, con)
  df = df.sort_values(by=col_of_scores, ascending=sort_by_ascending)
  first_res_col_name, first_col_limit = cols_to_group_by[0], col_limits[0]
  if(keep_duplicates[0]==False):
    #drop duplicates in first column
    df = df.drop_duplicates(subset=[first_res_col_name])
  # only keep entries in first col that have at least
  # one row that they are in that is within the
  # col_limits[0]th topmost/bottomost entries
  # (top if sort_by_ascending==True, else
  # bottom). df2 stores the names of these entries of the first column.
  df2 = df.groupby(first_res_col_name)[col_of_scores].max().to_frame()
  # make sure still sorted by col_of_scores, and grab
  # number of rows given by first limit specified in col_limits
  df2 = df2.sort_values(by=col_of_scores, ascending=False).head(first_col_limit)
  # restrict to only those with that topmost/bottomost col_limits[0]th
  # entries in overall dataframe (df) being used here.
  df = df[df[first_res_col_name].isin(df2.index.to_frame()[first_res_col_name])]

  #for each subsequent index (>= 1) in col_limits and keep_duplicates,
  #further restrict the table, going to its right.
  subsets_cols_to_group_by = [cols_to_group_by[0:(i+1)] for i in range(len(cols_to_group_by))]
  #subset list to group by at any given iteration is always at index one greater
  #than subset list for droping duplicates
  for ind in range(len(cols_to_group_by[1:])):
    keep_duplicates_in_cur_col = keep_duplicates[ind+1]
    cur_cols_duplicate_list = subsets_cols_to_group_by[ind+1]
    cur_cols_group_by_list = subsets_cols_to_group_by[ind]
    cur_limit = col_limits[ind+1]
    if not(keep_duplicates_in_cur_col):
      #drop duplicates in current column
      df = df.drop_duplicates(subset=cur_cols_duplicate_list)
    #limit number of entries in column by highest/lowest num groupings,
    #given by cur_limit
    df.groupby(by=cur_cols_group_by_list, group_keys=False).head(cur_limit)
  
  #construct resulting table name
  res_table_name = f'{input_table_name}_grouped_by_'
  cols_limited_as_strs = [str(i) for i in col_limits]
  cols_limited_with_eqs = [cols_to_group_by[i] + "_" + str(cols_limited_as_strs[i]) + "_" + str(keep_duplicates[i]) for i in range(len(col_limits))]
  cols_delimited_with_eqs_as_str = '_'.join(cols_limited_with_eqs)
  res_table_name += cols_delimited_with_eqs_as_str
  res_table_name += res_table_suffix #add suffix
  #write resulting table to database, and return its table name
  df.to_sql(res_table_name, con, if_exists='replace')
  return res_table_name

def get_basic_stats_of_col1_grouped_by_cols(cur, con,
  col_name, cols_to_group_by, input_table_name=db_constants.WINE_INIT_TABLE_NAME, 
  res_table_suffix="",
  stats_to_compute=['count', 'min', 'max', 'mean', 'median', 'std']):
  """Creates a new table from the input table name,
  grouped by the column names passed, that gives the statistical
  functions passed computed on that column by those groups, and writes
  this new table to the database. Currently the name of this table
  is auto-generated here, but you can append to this name
  if you like via res_table_suffix.
  Param:
   @cur, con: database vars
   @col_name: the numeric column to calculate basic statistics
   on. Note that this column must be numeric for this function
   to work.
   @cols_to_group_by: a list of column names (as strings) to 
   group by. Note that the order of column names in this list
   matters: values are grouped in the order from first element
   of this list to last element.
   @input_table_name: the name of the table these
   columns for col_name and cols_to_group_by are in
   @res_table_suffix: a suffix to add to the end of the
   resulting table created with these statistics computed
   on col_name
   @stats_to_compute: a list of functions to compute statistics
   for on that numeric column col_name. If implemented in pandas,
   then that function can be its string function name.
    - For more info on what to pass to stats_to_compute,
      look at the info for pandas.DataFrame.aggregate:
      https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.aggregate.html.
      This argument is just the list of functions argument to
      aggregate(), so whatever goes for that list for it also
      applies here on what stats_to_compute may be.
  The resulting table has the columns for statistics have
  the same names as the names of the functions used to compute them
  (currently assumes pandas functions, though might work otherwise).
  Note that this function will overwrite a table of the same name
  if it already exists.
   
  Ex: if table_name is the table name for the null-cleaned
  overall wine table (currently wine_init), 
  the column to calculate these statistics on is
  col_name='price', and the columns to group by is
  ['country', 'region_1'], with no res_table_suffix
  specified, then this function creates a table named
  'price_basic_stats_grouped_by_country_region_1'
  with columns 'country' and 'region_1' followed
  by the stats for each. If res_table_suffix were
  'stats_table', for example, then the resulting table
  name for the created table would instead be
  'price_basic_stats_grouped_by_country_region_1_stats_table',
  for example.

   Returns the name (as a string) of this newly-created 
   table of basics statistics.
  """ 
  assert isinstance(col_name, str)
  assert isinstance(cols_to_group_by, list)
  assert len(cols_to_group_by) > 0
  for col in cols_to_group_by:
    assert isinstance(col, str)
  assert isinstance(input_table_name, str)
  assert isinstance(res_table_suffix, str)
  assert isinstance(stats_to_compute, list)

  #construct resulting table name for stats in database to create
  str_cols_grouped_by = ""
  for col_to_group_by in cols_to_group_by:
    str_cols_grouped_by += "_" + col_to_group_by
  # add undescore for result table name's suffix if it is specified
  res_table_suffix = res_table_suffix if not(res_table_suffix) else f'_{res_table_suffix}' 
  res_table_name = f'{col_name}_basic_stats_grouped_by{str_cols_grouped_by}'

  #read in table to data frame
  pd_table = pd.read_sql(f'SELECT * FROM {input_table_name}', con)
  #group by the columns in cols_to_group_by
  pd_res = pd_table.groupby(cols_to_group_by)[col_name].aggregate(stats_to_compute)
  #output to database
  pd_res.to_sql(res_table_name, con, if_exists='replace')
  #return the newly-created table's name
  return res_table_name