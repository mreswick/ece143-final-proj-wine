import os
import numpy as np
import json
import re
from tqdm import tqdm
def load_word():
    '''
    load description and cleaning the string
    '''
    file= open('data/winemag-data-130k-v2.json',encoding='utf-8')
    words_list = []
    s = file.readline()
    jdata = json.loads(s)
    l=0
    count=0
    for data in jdata:
        if data["points"]==None:
            continue
        if data["description"]==None:
            continue
        label=int(data["points"])
        # remove punctuation
        r = '[’!"#$%&\'()*+,-./:;<=>?@[\\]^_`{|}~\n。！，]+'
        text1=str(data["description"])
        text1 = text1.replace('\n', '')
        text1 = text1.replace('<br /><br />', ' ')
        text1 = re.sub(r, '', text1)
        text1 = text1.split(' ')
        text1 = [text1[i].lower() for i in range(len(text1)) if text1[i] != '']
        words_list.append([text1, label])
        l+=len(text1)
        count+=1
    average=l/count
    print(average)
    file.close()
    del s,jdata
    return words_list

def get_word_vector(vocabulary_path,word_list_path,dim):
    '''
    represent description in word vector
    input : path of vocabulary_path: glove vector
            path of word_list_path: cleaned description
            dim: dimension of word vector
    output: training data and testing data
    '''
    vocabulary_vectors = np.load(vocabulary_path, allow_pickle=True)
    word_list = np.load(word_list_path, allow_pickle=True)
    word_list = word_list.tolist()
    data = load_word()
    word_vector=[]
    labels=[]
    count=0
    for i in tqdm(range(len(data))):
        sentence = data[i][0]
        labels.append(data[i][1])
        temp = []
        index = 0
        for j in range(len(sentence)):
            try:
                index = word_list.index(sentence[j])
            except ValueError:
                index = 399999
            finally:
                temp.append(list(vocabulary_vectors[index]))
        if len(temp) < 30:
            for k in range(len(temp), 30):
                temp.append([0]*50)
        else:
            temp = temp[0:30]
        word_vector.append(temp)
        count+=1
        if count>90000:
            break
    np.save('data/train_data_269'+str(dim), np.array(word_vector[:int(len(word_vector)*0.8)]))
    np.save('data/train_label_269'+str(dim), np.array(labels[:int(len(word_vector)*0.8)]))
    np.save('data/test_data_269' + str(dim), np.array(word_vector[int(len(word_vector)*0.8):]))
    np.save('data/test_label_269' + str(dim), np.array(labels[int(len(word_vector)*0.8):]))



if __name__=="__main__":
    get_word_vector('data/vocabulary_vectors_50d.npy','data/word_list_50d.npy',50)

