import os
import numpy as np
import json
import re
from tqdm import tqdm
def load_word():
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
        # 去除标点符号
        r = '[’!"#$%&\'()*+,-./:;<=>?@[\\]^_`{|}~\n。！，]+'
        text1=str(data["description"])
        #text2=str(data["review_summary"])
        text1 = text1.replace('\n', '')
        text1 = text1.replace('<br /><br />', ' ')
        text1 = re.sub(r, '', text1)
        text1 = text1.split(' ')
        text1 = [text1[i].lower() for i in range(len(text1)) if text1[i] != '']
        #text2 = text2.replace('\n', '')
        #text2 = text2.replace('<br /><br />', ' ')
        #text2 = re.sub(r, '', text2)
        #text2 = text2.split(' ')
        #text2 = [text2[i].lower() for i in range(len(text2)) if text2[i] != '']
        words_list.append([text1, label])
        l+=len(text1)
        count+=1
    average=l/count
    print(average)
    file.close()
    del s,jdata
    return words_list

def get_word_vector(vocabulary_path,word_list_path,dim):
    #sequence length=50*
    vocabulary_vectors = np.load(vocabulary_path, allow_pickle=True)
    word_list = np.load(word_list_path, allow_pickle=True)
    word_list = word_list.tolist()
    data = load_word()
    word_vector=[]
    labels=[]
    count=0
    for i in tqdm(range(len(data))):
        # print(i)
        sentence = data[i][0]
        labels.append(data[i][1])
        temp = []
        index = 0
        for j in range(len(sentence)):
            try:
                index = word_list.index(sentence[j])
            except ValueError:  # 没找到
                index = 399999
            finally:
                temp.append(list(vocabulary_vectors[index]))  # temp表示一个单词在词典中的序号
        if len(temp) < 30:
            for k in range(len(temp), 30):  # 不足补0
                temp.append([0]*50)
        else:
            temp = temp[0:30]  # 只保留250个
        word_vector.append(temp)
        count+=1
        if count>90000:
            break
    
    # print(sentence_code)
    #train_data=word_vector[:int(len(word_vector)*0.8)]
    #train_label=labels[:int(len(word_vector)*0.8)]
    #test_data=word_vector[int(len(word_vector)*0.8):]
    #test_label=labels[int(len(word_vector)*0.8):]

    #train_data = np.array(train_data)
    #train_label=np.array(train_label)
    #test_data=np.array(test_data)
    #test_label=np.array(test_label)
    np.save('data/train_data_269'+str(dim), np.array(word_vector[:int(len(word_vector)*0.8)]))
    np.save('data/train_label_269'+str(dim), np.array(labels[:int(len(word_vector)*0.8)]))
    np.save('data/test_data_269' + str(dim), np.array(word_vector[int(len(word_vector)*0.8):]))
    np.save('data/test_label_269' + str(dim), np.array(labels[int(len(word_vector)*0.8):]))



if __name__=="__main__":
    #get word vector with embedding dimension of 50*
    #get_word_vector('data/vocabulary_vectors_50d.npy','data/word_list_50d.npy',50)
    get_word_vector('data/vocabulary_vectors_100d.npy','data/word_list_100d.npy',100)

