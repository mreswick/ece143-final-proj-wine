import torch
import torch.nn as nn
from torch.nn import functional as F
import math

class MainNet(nn.Module):
  def __init__(self,len_data,dim_in,dim_k):
    super(MainNet,self).__init__()
    self.MHSA=MultiHeadSelfAttention(len_data,dim_in,dim_k,dim_k)
    self.FFN=FFN(dim_k)
    self.simple_attention=SimpleAttention(dim_k)
    self.layernorm=nn.LayerNorm(dim_k)
    self.dropout=nn.Dropout(p=0.5)
    self.fc_dim1=dim_k#sequnece length
    self.fc_dim2=32
    self.fc_dim3=16
    self.fc_dim4 = 11
    self.fc_dim5 = 11
    self.fc1 = nn.Linear(self.fc_dim1, self.fc_dim5)
    self.fc2 = nn.Linear(self.fc_dim2, self.fc_dim3)
    self.fc3 = nn.Linear(self.fc_dim3, self.fc_dim4)
    self.fc4 = nn.Linear(self.fc_dim4, self.fc_dim5)
    self.bn1 = nn.BatchNorm1d(num_features=self.fc_dim2, eps=1e-05, momentum=0.1, affine=True,
                              track_running_stats=True)
    self.bn2 = nn.BatchNorm1d(num_features=self.fc_dim3, eps=1e-05, momentum=0.1, affine=True,
                              track_running_stats=True)
    self.bn3 = nn.BatchNorm1d(num_features=self.fc_dim4, eps=1e-05, momentum=0.1, affine=True,
                              track_running_stats=True)
  
  def forward(self, x):
    x_mhsa=self.MHSA(x)
    x=self.layernorm(x+x_mhsa)
    x=self.dropout(x)
    x=self.FFN(x)
    #x=torch.reshape(x,(x.shape[0],-1))
    x=self.simple_attention(x)
    x=self.fc1(x)
    #x=F.relu(x)
    #x=self.bn1(x)
    #x=self.dropout(x)
    #x=self.fc2(x)
    #x=F.relu(x)
    #x=self.bn2(x)
    #x=self.dropout(x)
    #x=self.fc3(x)
    return x



class MultiHeadSelfAttention(nn.Module):
    dim_in: int  # input dimension
    dim_k: int   # key and query dimension
    dim_v: int   # value dimension
    num_heads: int  # number of heads, for each head, dim_* = dim_* // num_heads

    def __init__(self, length,dim_in, dim_k, dim_v, num_heads=2):
        super(MultiHeadSelfAttention, self).__init__()
        assert dim_k % num_heads == 0 and dim_v % num_heads == 0, "dim_k and dim_v must be multiple of num_heads"
        self.dim_in = dim_in
        self.dim_k = dim_k
        self.dim_v = dim_v
        self.num_heads = num_heads
        self.linear_q = nn.Linear(dim_in, dim_k, bias=False)
        self.linear_k = nn.Linear(dim_in, dim_k, bias=False)
        self.linear_v = nn.Linear(dim_in, dim_v, bias=False)
        self._norm_fact = 1 / math.sqrt(dim_k // num_heads)
        self.layer_norm=nn.LayerNorm(dim_k)
        self.dropout=nn.Dropout(p=0.5)


    def forward(self, x):
        # x: tensor of shape (batch, n, dim_in)
        batch, n, dim_in = x.shape
        assert dim_in == self.dim_in

        nh = self.num_heads
        dk = self.dim_k // nh  # dim_k of each head
        dv = self.dim_v // nh  # dim_v of each head

        q = self.linear_q(x).reshape(batch, n, nh, dk).transpose(1, 2)  # (batch, nh, n, dk)
        k = self.linear_k(x).reshape(batch, n, nh, dk).transpose(1, 2)  # (batch, nh, n, dk)
        v = self.linear_v(x).reshape(batch, n, nh, dv).transpose(1, 2)  # (batch, nh, n, dv)

        #attn_mask = attn_mask.unsqueeze(1).repeat(1, self.num_heads, 1, 1)

        dist = torch.matmul(q, k.transpose(2, 3)) * self._norm_fact  # batch, nh, n, n
        #dist=dist.masked_fill_(attn_mask, -1e9)
        dist = torch.softmax(dist, dim=-1)  # batch, nh, n, n
        dist=self.dropout(dist)
        att = torch.matmul(dist, v)  # batch, nh, n, dv
        att = att.transpose(1, 2).reshape(batch, n, self.dim_v)  # batch, n, dim_v
        #att_mask=att.eq(0)
        #att=att.masked_fill_(att_mask,-1e9)

        return att

    def get_attention_padding_mask(self, q):
        attn_pad_mask = q[:,:,0].eq(0).unsqueeze(1).repeat(1, q.size(1), 1)
        # |attn_pad_mask| : (batch_size, q_len, k_len)

        return attn_pad_mask

class FFN(nn.Module):
    #feed foward network
    def __init__(self,dim_v):
        super(FFN, self).__init__()
        self.d_ff=100
        self.w1=nn.Linear(dim_v,self.d_ff)
        self.w2=nn.Linear(self.d_ff,dim_v)
        self.layernorm=nn.LayerNorm(dim_v)
        self.dropout=nn.Dropout(p=0.5)


    def forward(self,x):
        residual=x
        x=self.w2(self.dropout(self.w1(x)))
        x=self.layernorm(x+residual)
        x=self.dropout(x)

        return x

class SimpleAttention(nn.Module):
    def __init__(self, input_dim):
        super(SimpleAttention, self).__init__()
        self.input_dim = input_dim
        self.scalar = nn.Linear(self.input_dim, 1, bias=False)

    def forward(self, M,x=None):
        """
        M -> (batch,seq_len,  vector)
        x -> dummy argument for the compatibility with MatchingAttention
        """
        scale = self.scalar(M)  #  batch,seq_len, 1
        #scale=scale.masked_fill_(attn_mask, -1e9)
        alpha = F.softmax(scale, dim=1).permute(0, 2, 1)  # batch, 1, seq_len
        attn_pool = torch.bmm(alpha, M)[:, 0, :]  # batch, vector

        return attn_pool