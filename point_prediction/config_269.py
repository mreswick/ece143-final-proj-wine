import torch.nn as nn

class parameters():
    batchsize=32
    lr=0.001
    epochs=100
    #criterion = nn.CrossEntropyLoss()
    criterion=nn.MSELoss(size_average=None, reduce=None, reduction='mean')