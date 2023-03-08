import re
from collections import defaultdict
from nltk.stem import PorterStemmer
from fuzzywuzzy import fuzz
import json
import math
import os

stopwords = ['i', 'me', 'my', 'myself', 'we', 'our', 'ours', 'ourselves', 'you', 'your', 'yours', 'yourself', 
    'yourselves', 'he', 'him', 'his', 'himself', 'she', 'her', 'hers', 'herself', 'it', 'its', 'itself', 
    'they', 'them', 'their', 'theirs', 'themselves', 'what', 'which', 'who', 'whom', 'this', 'that', 'these', 
    'those', 'am', 'is', 'are', 'was', 'were', 'be', 'been', 'being', 'have', 'has', 'had', 'having', 'do', 
    'does', 'did', 'doing', 'a', 'an', 'the', 'and', 'but', 'if', 'or', 'because', 'as', 'until', 'while', 'of',
    'at', 'by', 'for', 'with', 'about', 'against', 'between', 'into', 'through', 'during', 'before', 'after', 
    'above', 'below', 'to', 'from', 'up', 'down', 'in', 'out', 'on', 'off', 'over', 'under', 'again', 'further', 
    'then', 'once', 'here', 'there', 'when', 'where', 'why', 'how', 'all', 'any', 'both', 'each', 'few', 'more', 
    'most', 'other', 'some', 'such', 'no', 'nor', 'not', 'only', 'own', 'same', 'so', 'than', 'too', 'very', 's', 
    't', 'can', 'will', 'just', 'don', 'should', 'now']

def find_similar_phrases(phrases, stopwords=stopwords, threshold = 80, generic = False):
    """
    Given a list of unique strings, find phrases that are similar to each other
    Param:
        @phrases: a list of unique strings
        @stopwords: stopwords to be removed
        @threshols: 0-100. Higher the value, more similar
        @generic: If true, then 'White Blanc':['Bordeaux-style White Blend',
                                                      'Alsace white blend',
                                                      'Rhône-style White Blend',
                                                      'Austrian white blend',
                                                      'Provence white blend']
    return:
        A dictionary similar_map that similar_map[phrase] = list of phrases to the key
    """
    
    #remove punctuations
    pattern = re.compile(r'[^\w\s]')
    phrases_cleaned = [pattern.sub('',str(s).lower()) for s in phrases]
    
    #remove stopping words, such as of, is, and, I, am, ...
    phrases_cleaned = [ ' '.join([i for i in s.split() if i not in stopwords]) for s in phrases_cleaned]

    #stemming (no need lemmentization)
    ps = PorterStemmer()
    phrases_cleaned = [' '.join([ps.stem(i) for i in s.split()]) for s in phrases_cleaned]
    
    #phrase by phrase comparison
    similar_map = defaultdict(list)
    for i in range(len(phrases_cleaned)):
        for j in range(i+1, len(phrases_cleaned)):
            p_a, p_b = phrases_cleaned[i], phrases_cleaned[j]
            if not generic:
                ratio = fuzz.token_sort_ratio(p_a, p_b) # using set_ratio since order does not matter
            else:
                ratio = fuzz.token_set_ratio(p_a, p_b) # generic grouping
            if ratio >= threshold:
                similar_map[phrases[i]].append(phrases[j])
                similar_map[phrases[j]].append(phrases[i])
    
    return similar_map

def mapping_dict (similar_map, generic = False, generic_thres=50):
    """
    Given a similar_map from find_similar_phrases, return mapping_dict
    
    Generic_thres: Min of similarity within same group
    """
    groups = []
    for k, v in similar_map.items():
        # continue if k was already searched before
        searched = False
        for g in groups:
            if k in g:
                searched = True
                break
        
        if searched:
            continue
        
        # find a group that contains similar phrases
        g = set(v+[k])
        prev_len = len(g)
        while len(g) > prev_len:
            prev_len = len(g)
            tmp = set()
            for p in g:
                tmp = tmp | similar_map[p]
            g = g | tmp
        
        if generic:
            groups.append(g)
            continue
        
        #check current group have any intersection with previous ones
        is_new=True
        for idx in range(len(groups)):
            if len(groups[idx] & g) > 0:
                groups[idx] = groups[idx] | g
                is_new=False
                break
                    
        if is_new:
            groups.append(g)
    
    # extra regrouping for generic to avoid repeating phrases across groups
    # If repeated phrases encountered, assign the phrase to its most similar group
    if generic:
        for k, _ in similar_map.items():
            
            # find the group idx that is best for k
            scores = []
            for idx in range(len(groups)):
                if k in groups[idx]:
                    score_tmp = [fuzz.token_set_ratio(k, p) for p in groups[idx]]
                    scores.append( (idx, sum(score_tmp)/len(score_tmp) ) )
            max_idx, _ = max(scores, key=lambda x: x[1])
            
            # delete k from other groups
            for idx, _ in scores:
                if idx != max_idx:
                    groups[idx].remove(k)
        
        singles = []
        for g in groups:
            if len(g) == 1:
                singles.append(list(g)[0])
        
        groups = [g for g in groups if len(g) > 1]
        for k in singles:
            # find the group idx that is best for k
            scores = []
            for idx in range(len(groups)):
                #print(groups[idx])
                score_tmp = [fuzz.token_set_ratio(k, p) for p in groups[idx]]
                scores.append( (idx, sum(score_tmp)/len(score_tmp) ) )
            max_idx, _ = max(scores, key=lambda x: x[1])
            
            groups[max_idx] = groups[max_idx] | {k}

    # contruct mapping dict based on the groups. Pick the shortest as the mapped value
    mapping_dict = {}
    for g in groups:
        key = min(g, key=lambda x: len(x))
        mapping_dict[key] = list(g)
        
    return mapping_dict

def check_not_nan(x):
    """
    check if x is nan
    """
    if isinstance(x, (int, float)):
        return not math.isnan(x) 
    return True

def mapping_column(col, threshold=85, generic=False, logging=True):
    """
    Given the column of df, return new column that similar phrases are mapped together
    Param:
        @col: the column of df, column can have nan, but must contains only str or Nan
        @threhold: 0-100, higher the threshold, high similarity
        @logging: If ture, output a json file that contains the mapping dictionary.
                    Filename would be mapping_{column_name}_{threshold}_{generic/''}.json
                    Ex of mapping dictionary:
                    {'Pinot Gris': ['Pinot Gris', 'Pinot Grigio'], 
                    'Riesling': ['Riesling', 'White Riesling', 'Johannisberg Riesling']}
                    Above dictionary means that Pinot Gris and Pinot Grigio will be mapped as Pinot Gris,
                    and 'Riesling', 'White Riesling', 'Johannisberg Riesling' will be mapped as Riesling
        
        @generic: If True, phrases will be mapped to broader, more generic category.
                    Ex: If generic == True, mapping dict will be:
                    {{'White Blend': ['Provence white blend', 'White Blend', 'Rhône-style White Blend', 'Bordeaux-style White Blend'
                    , 'Alsace white blend', 'Austrian white blend'], 
                    'Pinot Gris': ['Pinot Gris', 'Pinot Grigio'], 
                    'Riesling': ['Riesling', 'White Riesling', 'Johannisberg Riesling'], ...}
                    
                    If generic == False, White Blend, Riesling will not be in mapping_dict:
                    {{'Portuguese Red': ['Portuguese Rosé', 'Portuguese Red'], 
                    'Pinot Gris': ['Pinot Gris', 'Pinot Grigio'], 
                    'Cabernet Sauvignon': ['Merlot-Cabernet Sauvignon', 'Cabernet Sauvignon-Merlot-Shiraz', 
                    'Cabernet Sauvignon-Merlot', 'Cabernet Sauvignon', ...], ... }

    Usage Example:
        mapping_column(df['variety'], threshold=85, generic=False, logging=True)
        mapping_column(df['variety'], threshold=85, generic=True, logging=True)
        
        mapping_column(df['region_1'], threshold=95, generic=False, logging=True)
                    
    """
    phrases = list(filter(check_not_nan, col.unique()))
    assert all([isinstance(p, str) for p in phrases])
    
    json_fn = f"./mapping_{col.name}_{threshold}{'_genenric' if generic else ''}.json"
    if os.path.isfile(json_fn):
        print("Loading existing mapping dictionary")
        with open(json_fn, 'r') as file:
            mapping = json.load(file)
    else:
        print("Creating new mapping dictionary")
        d = find_similar_phrases(phrases, threshold=threshold, generic=generic)
        print("Re-grouping")
        mapping = mapping_dict(d, generic=generic)
    
    if logging and (not os.path.exists(json_fn)):
        with open(json_fn, 'w') as file:
            file.write( json.dumps(mapping, ensure_ascii=False).encode('utf8').decode() )
    
    one = {}
    for k, v in mapping.items():
        for v_ in v:
            one[v_] = k
            
    return col.apply(lambda x: one[x] if (check_not_nan(x) and x in one) else x)
    
    