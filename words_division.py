# -*- coding: utf-8 -*-
#本程序主要用于jieba分词，以及去除停用词

import os
import jieba


# 保存文件的函数
def savefile(savepath,content):
    fp = open(savepath,'w',encoding='utf-8',errors='ignore')
    fp.write(content)
    fp.close()

# 读取文件的函数
def readfile(path):
    fp = open(path, "r", encoding='utf-8', errors='ignore')
    content = fp.read()
    fp.close()
    return content

## 去除停用词的2个函数
# 创建停用词list
def stopwordslist(filepath):
    stopwords = [line.strip() for line in open(filepath, 'r', encoding='utf-8').readlines()]
    return stopwords

# 对句子去除停用词
def movestopwords(sentence):
    stopwords = stopwordslist('语料/bd_stop_words.txt')  # 这里加载停用词的路径
    outstr = ''
    for word in sentence:
        if word not in stopwords:
            if word != '\t'and'\n':
                outstr += word
                # outstr += " "
    return outstr

def movestopwordslist(list):
    stopwords = stopwordslist('语料/bd_stop_words.txt')
    outlist = []
    for word in list:
        #print (word)
        if word not in stopwords:
            outlist.append(word)
    return outlist

corpus_path = "语料/train_text/"  # 未分词分类预料库路径
seg_path = "语料/train_seg/"  # 分词后分类语料库路径

catelist = os.listdir(corpus_path)  # 获取未分词目录下所有子目录

wordlist = []

with open ("wordlist.csv",'w') as Wfile:
    for mydir in catelist:
        class_path = corpus_path + mydir + "/"  # 拼出分类子目录的路径
        seg_dir = seg_path + mydir + "/"  # 拼出分词后预料分类目录
        if not os.path.exists(seg_dir):  # 是否存在，不存在则创建
            os.makedirs(seg_dir)

        file_list = os.listdir(class_path) # 列举当前目录所有文件
        for file_path in file_list:
            fullname = class_path + file_path # 路径+文件名
            print("当前处理的文件是： ",fullname)  # 语料/train/pos/pos1.txt
                            #  语料/train/neg/neg1.txt

            content = readfile(fullname).strip()  # 读取文件内容
            content = content.replace("\n", "").strip()  # 删除换行和多余的空格
            content_seg = jieba.cut(content)    # jieba分词
            #print("jieba分词后：",list(content_seg))
            '''
            listcontent = ''
            for i in content_seg:
                listcontent += i
                listcontent += " "
            #print(listcontent[0:10])
            '''
            words = list(content_seg)
            newlist = movestopwordslist(words)
            for word in newlist:
                if(word != ' '):
                    wordlist.append(word)
            '''
            listcontent = movestopwords(listcontent)    # 去除停用词
            #print("去除停用词后：", listcontent[0:10])
            listcontent = listcontent.replace("   ", " ").replace("  ", " ")
            savefile(seg_dir + file_path, "".join(listcontent)) # 保存
            '''
print (wordlist)