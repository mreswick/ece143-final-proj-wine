import os
import numpy as np
import torch
from torch.utils.data.dataset import Dataset
from torch.utils.data.dataloader import DataLoader
from torchvision import transforms
device = torch.device('cuda:0' if torch.cuda.is_available() else 'cpu')

class TextDataset(Dataset):
    def __init__(self,data_dir,label_dir):
        self.data_dir=data_dir
        self.label_dir=label_dir
        self.data = np.load(self.data_dir, allow_pickle=True)
        print('shape of data:'+str(self.data.shape))
        self.label=np.load(self.label_dir,allow_pickle=True)

    def __len__(self):
        data=np.load(self.data_dir,allow_pickle=True)
        return len(data)

    def __getitem__(self,idx):
        return self.data[idx],self.label[idx]

if __name__=="__main__":
    #see shape of data here
    train_data=TextDataset('/content/drive/Shareddrives/CSE258/data/train_data50.npy','/content/drive/Shareddrives/CSE258/data/train_label50.npy')
    train_loader=DataLoader(dataset=train_data,batch_size=32)
    for data,label in train_loader:
        #data = torch.from_numpy(data)
        data.to(device=device, dtype=torch.float32)
        print(label.shape)
        print(data.shape)