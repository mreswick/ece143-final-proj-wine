# point_prediction:
Contains the neural network and related functions to predict point ratings of wine based on the descriptions for the reviews of wines we have. The current accuracy is approximately 60% (0.5934). This was achieved with a feature length vector of length 50. If we had more compute power, than we would likely have used a longer vector (300 is often standard), which would likely have noticeably increased our accuracy. However, even with this accuracy, the resulting distribution of predicted points closely matches that of the actual. The main difficulty the model had was predicting points at the extremes of the point range (such as for wines that had very high points or a perfect score (100)).
- 1 Run load_glove_data.py to get vocabulary vectors
- 2 Run get_word_vector.py to get word embeddings from vocabulary vectors
- 3 Run train_and_test.py to train and test
- Please download glove data here: https://nlp.stanford.edu/projects/glove/
- Create a new directory under the root path name "data" and put unzipped golve data into it
<center><img src="https://github.com/mreswick/ece143-final-proj-wine/blob/main/point_prediction/network.png" width="60%"></center>

## result:
<center><img src="https://github.com/mreswick/ece143-final-proj-wine/blob/main/point_prediction/acc_0.5934_predict.png" width="60%"></center>
<center><img src="https://github.com/mreswick/ece143-final-proj-wine/blob/main/point_prediction/acc_0.5934_true.png" width="60%"></center>


