import numpy as np
from torch.utils.data.dataset import Dataset
from torch.utils.data.dataloader import DataLoader
from data_loader_143 import TextDataset
from config_143 import parameters
import torch
from tqdm import tqdm
from network_143 import MainNet
import time

MainNet=MainNet(50,50,50)
parameters=parameters()
device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
train_data_path='/content/drive/Shareddrives/CSE258/data/train_data50.npy'
train_label_path='/content/drive/Shareddrives/CSE258/data/train_label50.npy'
test_data_path='/content/drive/Shareddrives/CSE258/data/test_data50.npy'
test_label_path='/content/drive/Shareddrives/CSE258/data/test_label50.npy'

def train_and_test(model):
    train_data=TextDataset(train_data_path,train_label_path)
    train_loader=DataLoader(dataset=train_data,batch_size=parameters.batchsize)
    test_data=TextDataset(test_data_path,test_label_path)
    test_loader=DataLoader(dataset=test_data,batch_size=parameters.batchsize)
    for epoch in tqdm(range(parameters.epochs)):
        print('current epoch:'+str(epoch))
        optimizer = torch.optim.Adam(model.parameters(), lr=parameters.lr)
        epoch_loss=0
        train_loss=0
        train_acc=0
        test_acc=0
        count=0
        corr=0
        for data,label in train_loader:
            #data size:(batch_size,num_words,num_dim)
            #label size:(batch_size)
            label_num=label
            #label.to(torch.float32)
            #label=label.unsqueeze(1)
            label=torch.nn.functional.one_hot(label.to(torch.int64),11)
            model=model.to(device)
            model.train()
            x=data.to(device=device,dtype=torch.float32)
            y=model(x)
            optimizer.zero_grad()
            loss = parameters.criterion(y, label.to(device=device).float())
            loss=loss.to(device=device,dtype=torch.float32)
            loss.backward()
            optimizer.step()
            train_loss+=torch.nn.functional.mse_loss(y,label.to(device=device).float())
            #epoch_loss += loss.cpu().detach().numpy().tolist()
            #y = y.cpu().detach().numpy().tolist()
            count+=1
            #for n in range(len(y)):
              #count+=1
              #if y[n].index(max(y[n])) == label_num.numpy().tolist()[n]:
                  #corr += 1
        train_acc=corr/count
        epoch_loss=epoch_loss/count
        train_loss/=count
        print('train mse='+str(train_loss))
        count=0
        corr=0
        test_loss=0
        for data,label in test_loader:
            label_num=label
            #label.to(torch.float32)
            #label=label.unsqueeze(1)
            label=torch.nn.functional.one_hot(label.to(torch.int64),11)
            model.eval()
            with torch.no_grad():
                x = data.to(device=device, dtype=torch.float32)
                y = model(x)
                y = y.cpu().detach()
                count+=1
                test_loss+=torch.nn.functional.mse_loss(y,label)
                #y = y.cpu().detach().numpy().tolist()
                #for n in range(len(y)):
                  #count+=1
                  #if y[n].index(max(y[n])) == label_num.numpy().tolist()[n]:
                    #corr += 1
        test_acc=corr/count
        test_loss/=count
        print('test mse='+str(test_loss))
        print('epoch:'+str(epoch)+' '+'train_mse:'+str(train_loss)+' '+'test_mse:'+str(test_loss))

if __name__=="__main__":
  train_and_test(MainNet)

