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
  def __init__(self, t_T=None):
    # self.a = a
    # self.rmin = rmin
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
    # if self[0].issuperset(t_T[0]):
    #   return T(self[0], max(self[1], t_T[1]))
    # else:
    #   return T(self[1]
    return to_ret
  def __lt__(self, other):
    return sum(self.values()) < sum(other.values())
  def __le__(self, other):
    return sum(self.values()) <= sum(other.values())
  # def __eq__(self, other):
  #   return sum(self.values()) == sum(other.values())
  # def __ne__(self, other):
  #   return sum(self.values()) != sum(other.values())
  def __eq__(self, other):
    return dict.__eq__(self, other)
  def __ne__(self, other):
    return not(self.__eq__(self, other))
  def __gt__(self, other):
    return sum(self.values()) > sum(other.values())
  def __ge__(self, other):
    return sum(self.values()) >= sum(other.values())

  def add_nparr(self, nparr):
    """nparr is 1D
    numpy array
    with distances, summed-squared,
    to circle centers in order
    of circle center indices.
    """
    for ind, x_y_sum_dist in np.ndenumerate(nparr):
      self[ind[0]] = x_y_sum_dist

class TextFilter:
    """A class that stores a dataframe and which
    column of text to process, as well as a 
    column that stores a list of the words
    of that text that have had stopping words removed
    and which have had stemming be done.

    This class can be used to create word clouds. To
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

        #print(self.d[coltext].head(5))

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

    # def split_str(self, text_entry):
    #     """Assumes the text entry is stored space-delimited,
    #     and so splits on comma to return list."""
    #     return text_entry.split(" ")
    # def congeal_str(self, list_text):
    #     """Takes a list of text words and joins them
    #     on a comma."""
    #     return " ".join(list_text)
    def set_adjectives(self, colname=None, suffix="adjectives"):
        """Uses TextBlob to filter for only adjectives."""
        colname = self.__coltext_cleaned if colname is None else colname
        # print("set_adjectives colname: ", colname)
        def get_adjectives(text):
            # print(type(text))
            # print(text)
            """Text to get the adjectives of."""
            word_blob = TextBlob(text)
            #following line inpsired by:
            # https://stackoverflow.com/questions/56980515/how-to-extract-all-adjectives-from-a-strings-of-text-in-a-pandas-dataframe
            #return [word for (word, tag) in word_blob.tags if tag.startswith("JJ")] 
            return [word for (word, tag) in word_blob.tags]
        #assumes colname text is comma-delimited, so split and process
        new_adj_col_name = f'{colname}_{suffix}'
        # print("new_adj_col_name: ", new_adj_col_name)
        # print(type(self.d[colname]))
        # print(self.d.dtypes)
        # print(self.d[colname].head(5))
        # print(self.d[colname])
        # print(self.d.head(5))
        self.d[new_adj_col_name] = self.d[colname].apply(get_adjectives)
    def set_adjectives_counts(self, colname=None, suffix="dict_counts"):
        """Adds a new column that is a dictionary with the counts of each
        word."""
        colname = f'{self.__coltext_cleaned}_adjectives' if colname is None else colname
        new_word_count_col_name = f'{self.__coltext_cleaned}_adjectives_{suffix}' if colname is None else f'{colname}_{suffix}'
        def create_word_count_dict(textlist):
            """List of words to get the counts of"""
            dict_words_seen = {}
            for word in textlist:
                dict_words_seen[word] = dict_words_seen.get(word, 0) + 1
            return T(dict_words_seen)
        self.d[new_word_count_col_name] = self.d[colname].apply(create_word_count_dict)
    # def group_by(self, cols_to_group_by, col_to_sum='description_processed_adjectives_dict_counts'):
    #     """Returns new dataframe with columns
    #     of columns to group by, along with "summed"
    #     custom type T dictionary of word count frequencies.
    #     """
    #     assert isinstance(cols_to_group_by, list)
    #     for col in cols_to_group_by:
    #         assert isinstance(col, str)
    #     df_to_ret = self.d.copy()
    #     df_to_ret = df_to_ret.groupby(by=cols_to_group_by, group_keys=False)[col_to_sum].sum().reset_index()
    #     #expand dataframe from dictionaries of words/keys to counts/values
    #     df_to_ret[col_to_sum + '_words'] = df_to_ret[col_to_sum].apply(lambda x: list(x.keys()))
    #     df_to_ret[col_to_sum + '_counts'] = df_to_ret[col_to_sum].apply(lambda x: list(x.values()))
    #     df_to_ret = df_to_ret.explode([col_to_sum + '_words', col_to_sum + '_counts'], ignore_index=True)
    #     #df_to_ret = df_to_ret.drop(columns=['temp1', 'temp2'])
    #     return df_to_ret
    def group_by(self, d, cols_to_group_by, col_to_sum='description_processed_adjectives_dict_counts'):
      """Returns new dataframe with columns
      of columns to group by, along with "summed"
      custom type T dictionary of word count frequencies.
      """
      assert isinstance(cols_to_group_by, list)
      for col in cols_to_group_by:
          assert isinstance(col, str)
      df_to_ret = d.copy()
      df_to_ret = df_to_ret.groupby(by=cols_to_group_by, group_keys=False)[col_to_sum].sum().reset_index()
      #expand dataframe from dictionaries of words/keys to counts/values
      df_to_ret[col_to_sum + '_words'] = df_to_ret[col_to_sum].apply(lambda x: list(x.keys()))
      df_to_ret[col_to_sum + '_counts'] = df_to_ret[col_to_sum].apply(lambda x: list(x.values()))
      df_to_ret = df_to_ret.explode([col_to_sum + '_words', col_to_sum + '_counts'], ignore_index=True)
      #df_to_ret = df_to_ret.drop(columns=['temp1', 'temp2'])
      return df_to_ret
    def group_by_dict(self, d, cols_to_group_by, col_to_sum='description_processed_adjectives_dict_counts'):
      #groups dict column as str, as dicts are not hashable for this comparison; convert
      #to str when removing it back out
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
      #assert df_to_measure_by is not None or tablename_to_measure_by is not None
      #assert (df_to_measure_by is None) or (tablename_to_measure_by is None)
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
          
    # def get_top_n_of_group_by(self, df, cols_to_group_by, n, col_summed='description_processed_adjectives_dict_count'):
    #   df_to_ret = df.groupby(by=cols_to_group_by, group_keys=False)[col_summed].head(n)
    #   return df_to_ret
    # def set_sorted_counts_after_group_by(self, df, colname='description_processed_adjectives_dict_count', suffix='sorted'):
    #     new_colname = f'{colname}_{suffix}'
    #     #creates new column in dataframe that is sorted list of tuples as (key, value)
    #     def get_sorted_counts(dict_counts):
    #       return sorted(dict_counts.items(), key=lambda x: x[0])
    #     df[newcolname] = df[colname].apply(get_sorted_counts)
    # def get_top_n_sorted_counts_after_group_by(self, df, n, colname='description_processed_adjectives_dict_count_sorted'):
    #   assert isinstance(n, int)
    #   assert isinstance(colname, str)
    #   newcolname = f'{colname}_top_{str(n)}'
    #   def get_top_n(list_counts):
    #     return list_counts[:n]
    #   df[newcolname] = df[colname].apply(get_top_n)
    # def get_top_n_sorted_counts_after_group_by_as_df(self, df, n, cols_grouped_by, colname='description_processed_adjectives_dict_count_sorted'):
    #   assert isinstance(cols_grouped_by, list)
    #   for col in cols_grouped_by:
    #     assert isinstance(col, str)
    #   suffix = f"_top_{str(n)}"
    #   colname += suffix
    #   dict_to_create_new_df_with = {}
    #   for col in cols_grouped_by + [colname]:
    #     dict_to_create_new_df_with[col] = []


    #   for index, row in df[[cols_grouped_by + [colname]]].iterrows():
    #     for (ind, value) in row.items():
    #       dict_to_create_new_df_with[ind] = value

    #   for (key, counts_list) in df[colname].items():


    
        

        
        
    

