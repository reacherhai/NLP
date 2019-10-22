import torch
from torch import nn
from torchtext import data
from torchtext import datasets
from torchtext.vocab import GloVe
import numpy as np
import os

def load_data(opt):
    # use torchtext to load data, no need to download dataset
    print("loading {} dataset".format(opt.dataset))
    # set up fields
    text = data.Field(lower=True, include_lengths=True, batch_first=True, fix_length=opt.max_seq_len)
    label = data.Field(sequential=False)

    # make splits for data
    train, test = datasets.IMDB.splits(text, label)

    # build the vocabulary
    text.build_vocab(train, vectors=GloVe(name='6B', dim=300))
    label.build_vocab(train)

    # print vocab information
    print('len(TEXT.vocab)', len(text.vocab))
    print('TEXT.vocab.vectors.size()', text.vocab.vectors.size())


cache = 'mycache'
if not os.path.exists(cache):
    os.mkdir(cache)
vectors = Vectors(name='/Users/wyw/Documents/vectors/glove/glove.6B.300d.txt', cache=cache)
# 指定 Vector 缺失值的初始化方式，没有命中的token的初始化方式
vectors.unk_init = init.xavier_uniform_
TEXT.build_vocab(train, min_freq=5, vectors=vectors)
# 查看词表元素
TEXT.vocab.vectors