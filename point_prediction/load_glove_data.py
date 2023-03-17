import os
import numpy as np

def load_cab_vector(dim):
    '''
    load glove word vector
    input: dim: int, word vector dimension
    output: word vector vocabulary
    '''
    word_list = []
    vocabulary_vectors = []
    data = open('/data/glove.6B/glove.6B.'+str(dim)+'d.txt', encoding='utf-8')
    for line in data.readlines():
        temp = line.strip('\n').split(' ')
        name = temp[0]
        word_list.append(name.lower())
        vector = [temp[i] for i in range(1, len(temp))]
        vector = list(map(float, vector))
        vocabulary_vectors.append(vector)
    # save as .npy
    vocabulary_vectors = np.array(vocabulary_vectors)
    word_list = np.array(word_list)
    np.save('data/vocabulary_vectors_'+str(dim)+'d', vocabulary_vectors)
    np.save('data/word_list_'+str(dim)+'d', word_list)
    return vocabulary_vectors, word_list

if __name__=="__main__":
    load_cab_vector(50)

