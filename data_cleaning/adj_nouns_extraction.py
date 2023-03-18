import pandas as pd
import spacy
from tqdm import tqdm


# To generate data/adjectives_nouns.csv. Takes 20-30 minutes to run

#stopwords = ['i', 'me', 'my', 'myself', 'we', 'our', 'ours', 'ourselves', 'you', 'your', 'yours', 'yourself',
#  'yourselves', 'he', 'him', 'his', 'himself', 'she', 'her', 'hers', 'herself', 'it', 'its', 'itself',
#  'they', 'them', 'their', 'theirs', 'themselves', 'what', 'which', 'who', 'whom', 'this', 'that', 'these',
#  'those', 'am', 'is', 'are', 'was', 'were', 'be', 'been', 'being', 'have', 'has', 'had', 'having', 'do',
#  'does', 'did', 'doing', 'a', 'an', 'the', 'and', 'but', 'if', 'or', 'because', 'as', 'until', 'while', 'of',
#  'at', 'by', 'for', 'with', 'about', 'against', 'between', 'into', 'through', 'during', 'before', 'after',
#  'above', 'below', 'to', 'from', 'up', 'down', 'in', 'out', 'on', 'off', 'over', 'under', 'again', 'further',
#  'then', 'once', 'here', 'there', 'when', 'where', 'why', 'how', 'all', 'any', 'both', 'each', 'few', 'more',
#  'most', 'other', 'some', 'such', 'no', 'nor', 'not', 'only', 'own', 'same', 'so', 'than', 'too', 'very', 's',
#  't', 'can', 'will', 'just', 'don', 'should', 'now']
#
#def pre_process(sentence):
#  """
#  stopwords removal, punctuations removal, stemming/lemmentizing
#  """
#
#  #remove punctuations
#  pattern = re.compile(r'[^\w\s]')
#  sent_cleaned = pattern.sub('',str(sentence).lower())
#
#  #remove stopping words, such as of, is, and, I, am, ...
#  phrases_cleaned = ' '.join([i for i in sentence.split() if i not in stopwords])
#
#  #stemming (no need lemmentization)
#  lemmatizer = WordNetLemmatizer()
#  phrases_cleaned = [' '.join([lemmatizer.lemmatize(i) for i in s.split()]) for s in phrases_cleaned]
#
#  return sentence

# Load English tokenizer, tagger, parser and NER
nlp = spacy.load("en_core_web_sm")
#spacy.prefer_gpu()

tqdm.pandas()

def get_adjectives(text, tag_ = 'JJ'):
  blob = nlp(text)
  return [ token.lemma_ for token in blob if token.tag_ == tag_]


df = pd.read_csv('./data/winemag-data-130k-v2.csv')
#df['description_cleaned'] = df['description'].progress_apply(lambda x: pre_process(x))
df['description_adj_cleaned'] = df['description'].progress_apply(get_adjectives)
df['description_noun_cleaned'] = df['description'].progress_apply(lambda x: get_adjectives(x, tag_ = 'NN'))
df.to_csv('../data/adjectives_nouns.csv')
