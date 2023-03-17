import pandas as pd
import re
import nltk
nltk.download('punkt')
from nltk.stem import PorterStemmer
from data_cleaning import phrases_mapping
from database import db_op
from textblob import TextBlob
from PIL import Image
import numpy as np
from wordcloud import WordCloud
import matplotlib.pyplot as plt

class T(dict):
  """A class that behaves as a dictionary, 
  except that it also supports the addition
  operation, in which case it's values get 
  added. This is namely used to 
  for word frequencies, so that
  dictionaries representing word
  frequencies can be added."""
  def __init__(self, t_T=None):
    """Create new T object
    from that which is inputted. This 
    can either be a tuple, representing
    a single key and value, or a dictionary,
    or a numpy array.""" 
    if(not(t_T is None)):
      if isinstance(t_T, tuple):
        assert isinstance(t_T, tuple)
        assert len(t_T) == 2
        super().__init__()
        self[t_T[0]] = t_T[1]
      elif isinstance(t_T, dict):
        super().__init__(t_T)
      elif isinstance(t_T, np.ndarray):
        self.add_nparr(t_T)
    else:
      super().__init__()

  def __add__(self, t_T):
    """This implements the above-mentioned
    adding for a dictionary, in which
    entries for the same keys are added."""
    to_ret = None
    if isinstance(t_T, tuple):
      assert isinstance(t_T, tuple)
      assert len(t_T) == 2
      to_ret = T(self)# self.copy()
      circle_center, rmin = t_T[0], t_T[1]
      curr_dist_stored = self.get(circle_center, 0)
      r_to_use = max(rmin, curr_dist_stored)
      to_ret[circle_center] = r_to_use
    elif isinstance(t_T, T) or isinstance(t_T, dict):
      to_ret = T(self)
      # should only have one key-value pair
      if len(t_T) > 0:
        k, v = list(t_T.items())[0]
        circle_center, rmin = k, v
        curr_dist_stored = self.get(circle_center, 0)
        r_to_use = rmin + curr_dist_stored #max(rmin, curr_dist_stored)
        to_ret[circle_center] = r_to_use
    return to_ret
  
  """Comparison functions for whether an object
  of this class is less than (__lt__),
  less than or equal to (__le__), 
  equal (__eq__), not equal to (__ne__),
  greater than (__gt__), or greater than
  or equal to (__ge__) that of another instance
  of this class.""" 
  def __lt__(self, other):
    assert isinstance(other, T)
    return sum(self.values()) < sum(other.values())
  def __le__(self, other):
    assert isinstance(other, T)
    return sum(self.values()) <= sum(other.values())
  def __eq__(self, other):
    assert isinstance(other, T)
    return dict.__eq__(self, other)
  def __ne__(self, other):
    assert isinstance(other, T)
    return not(self.__eq__(self, other))
  def __gt__(self, other):
    assert isinstance(other, T)
    return sum(self.values()) > sum(other.values())
  def __ge__(self, other):
    assert isinstance(other, T)
    return sum(self.values()) >= sum(other.values())

class TextFilter:
    """A class that stores a dataframe and which
    column of text to process, as well as a 
    column that stores a list of the words
    of that text that have had stopping words removed
    and which have had stemming be done. This class
    thus represents a dataframe with text filtering
    performed on it, namely in regards to getting
    words and their frequencies and storing them,
    so that it can then create word clouds.

    This class can thus be used to create word clouds. To
    do so, after initializing an object of it:
      1. at minimum: TextFilter(df) with the 
      dataframe you wish to process with that column
      of text (coltext) in it. Generally, this is just
      the overall wine dataset dataframe. 
      2. Call produce_wine_word_clouds(...) with that
      object to produce word clouds grouped by a given
      set of columns, with the "top n" of rows
      grouped by those columns, as measured by what
      "top n" means per another dataframe passed in
      (such as top 10 by mean, top 10 by count, etc.),
      having word clouds produced for them.
    """ 
    def __init__(self, df, coltext='description', stopwords=phrases_mapping.stopwords, suffix="processed"):
        """Initialize class with dataframe and column of text,
        and use stopwords to clean out entries. By default
        do this on the description column.
        """ 
        assert isinstance(coltext, str)
        assert isinstance(stopwords, list)
        for word in stopwords:
            assert isinstance(word, str)
        assert isinstance(df, pd.DataFrame)
        #logic drawn from phrases_mapping.py beginning of
        #find_similar_phrases
        self.d = df.copy()
        self.__coltext = coltext
        self.__coltext_cleaned = f'{coltext}_{suffix}'

        #initialize description_processed column
        for (rowind, description) in self.d[coltext].items():
            #convert description to list of strings, separated on whitespace
            phrases = description.split()

            #remove 
            pattern = re.compile(r'[^\w\s]')
            phrases_cleaned = [pattern.sub('',str(s).lower()) for s in phrases]
            
            #remove stopping words, such as of, is, and, I, am, ...
            phrases_cleaned = [ ' '.join([i for i in s.split() if i not in stopwords]) for s in phrases_cleaned]

            #stemming (no need lemmentization)
            ps = PorterStemmer()
            phrases_cleaned = [' '.join([ps.stem(i) for i in s.split()]) for s in phrases_cleaned]

            #add column that is coltext_cleaned, as given above, that is space 
            #delimited string of these phrases
            self.d.loc[df.index[rowind], self.__coltext_cleaned] = ' '.join(phrases_cleaned)

        #initialize with adjectives list
        self.set_adjectives()
        self.set_adjectives_counts()
  
    def set_adjectives(self, colname=None, suffix="adjectives"):
        """Uses TextBlob to filter; by default, assumes
        filtering for adjectives.It writes the resulting list
        of words into a new column in the dataframe that
        this class wraps.
        Param:
          @colname: the column with the text to get.
          @suffix: the suffix to added to a new column name for the
          dictionary this class wraps.
        """
        assert colname is None or isinstance(colname, str)
        assert isinstance(suffix, str)
        colname = self.__coltext_cleaned if colname is None else colname
        # print("set_adjectives colname: ", colname)
        def get_words(text):
            """Text to get the words of."""
            word_blob = TextBlob(text)
            #following line inpsired by:
            # https://stackoverflow.com/questions/56980515/how-to-extract-all-adjectives-from-a-strings-of-text-in-a-pandas-dataframe
            return [word for (word, tag) in word_blob.tags]
        #assumes colname text is comma-delimited, so split and process
        new_adj_col_name = f'{colname}_{suffix}'
        self.d[new_adj_col_name] = self.d[colname].apply(get_words)
  
    def set_adjectives_counts(self, colname=None, suffix="dict_counts"):
        """Adds a new column that is a dictionary with the counts of each
        word, with the given column name with suffix appended to it, to
        the dataframe this class wraps."""
        assert colname is None or isinstance(colname, str)
        assert isinstance(suffix, str)
        colname = f'{self.__coltext_cleaned}_adjectives' if colname is None else colname
        new_word_count_col_name = f'{self.__coltext_cleaned}_adjectives_{suffix}' if colname is None else f'{colname}_{suffix}'
        def create_word_count_dict(textlist):
            """List of words to get the counts of"""
            dict_words_seen = {}
            for word in textlist:
                dict_words_seen[word] = dict_words_seen.get(word, 0) + 1
            return T(dict_words_seen)
        self.d[new_word_count_col_name] = self.d[colname].apply(create_word_count_dict)

    def group_by(self, d, cols_to_group_by, col_to_sum='description_processed_adjectives_dict_counts'):
      """Returns new dataframe with columns
      of columns to group by, along with "summed"
      custom type T dictionary of word count frequencies.
      """
      assert isinstance(d, pd.DataFrame)
      assert isinstance(col_to_sum, str)
      assert isinstance(cols_to_group_by, list)
      for col in cols_to_group_by:
          assert isinstance(col, str)
      df_to_ret = d.copy()
      df_to_ret = df_to_ret.groupby(by=cols_to_group_by, group_keys=False)[col_to_sum].sum().reset_index()
      #expand dataframe from dictionaries of words/keys to counts/values
      df_to_ret[col_to_sum + '_words'] = df_to_ret[col_to_sum].apply(lambda x: list(x.keys()))
      df_to_ret[col_to_sum + '_counts'] = df_to_ret[col_to_sum].apply(lambda x: list(x.values()))
      df_to_ret = df_to_ret.explode([col_to_sum + '_words', col_to_sum + '_counts'], ignore_index=True)
      return df_to_ret
    def group_by_dict(self, d, cols_to_group_by, col_to_sum='description_processed_adjectives_dict_counts'):
      """Groups dict column as str, as dicts are not hashable for this comparison; convert
      to str when removing it back out.
      This function groups by a column of dictionaries (col_to_sum), along with
      columns in the cols_to_group_by list, by first treating the dictionaries in col_to_sum
      as strings.
      This functon is used to group by dictionaries of frequencies when creating word clouds.
      Param:
        @d: a dataframe to group
        @cols_to_group_by: the column to group by
        @col_to_sum: the colum to convert to string and then group by with
      """
      assert isinstance(d, pd.DataFrame)
      assert isinstance(col_to_sum, str)
      assert isinstance(cols_to_group_by, list)
      for col in cols_to_group_by:
        assert isinstance(col, str)
      new_d = d.copy()
      new_d[col_to_sum] = new_d[col_to_sum].astype(str)
      new_d = new_d.groupby(by=cols_to_group_by + [col_to_sum], group_keys=False)[cols_to_group_by + [col_to_sum]].apply(lambda x: x).drop_duplicates()
      return new_d
    def produce_wine_word_clouds(
      self, 
      cur, 
      con, 
      cols_to_group_by,
      topn, 
      col_to_measure_by, 
      df_to_measure_by=None, 
      tablename_to_measure_by=None, 
      ascending=False, 
      col_to_sum='description_processed_adjectives_dict_counts', 
      img_file_path='visuals\wine-glass-outline-hi.png',
      colormapname='Reds', 
      **kwargs):
      """Produces wine word clouds for the 'top n' of
      groups (grouped by columns in cols_to_group_by) as measured
      by cols_to_measure_by in either the df or table to measure by.
      The column used to sum the frequency counts is col_to_sum,
      and img_file_path is used to give an overall background. The
      default coloring scheme is red.
      Param:
        @cur, con: database vars
        @cols_to_group_by: the columns to group by to produce the word clouds
        (ie one for each such group)
        @topn: the topn groups (only produes a word cloud for the topn
        such groups)
        @col_to_measure_by: the column to measure the "top n" by
        @df_to_measure_by, tablename_to_measure_by: either the dataframe
        or tablename with the column col_to_measure_by can be passed
        @ascending: whether topn gets the "top" or "bottom" n groups,
        as measured by the corresponding entry in col_to_measure_by
        @col_to_sum: the name for the column of summed frequencies.
        @img_file_path: a path to the image to use for the word clouds
        @colormapname: the name for the color map to be used for the
        word clouds
        @**kwargs: any extra arguments to pass to the word cloud creation.
      """
      assert isinstance(cols_to_group_by, list)
      for col in cols_to_group_by:
        assert isinstance(col, str)
      assert isinstance(topn, int)
      assert topn > 0
      df_to_plot = self.group_by(self.d, cols_to_group_by, col_to_sum)
      df_to_plot = self.group_by_dict(df_to_plot, cols_to_group_by, col_to_sum)
      #keep only the topn by some given measurement
      df_to_plot = db_op.filter_top_n(cur, con, 
        cols_to_group_by, 
        col_to_measure_by, 
        topn, 
        df=df_to_plot, 
        df_to_measure_by=df_to_measure_by, 
        tablename_to_measure_by=tablename_to_measure_by,
        ascending=ascending)
      mask = np.array(Image.open(img_file_path))
      #plot word cloud for each
      for (k, row) in df_to_plot.iterrows():
        print("key for row: ", k)
        print("row: ", row)
        wordcloud = WordCloud(mask=mask, width = 800, height = 800, colormap=plt.get_cmap(colormapname),
                    background_color ='white').generate_from_frequencies(eval(row[col_to_sum]), **kwargs)
        plt.imshow(wordcloud)
        plt.axis("off")
        plt.show()