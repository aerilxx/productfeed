#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Nov 12 13:32:15 2018

@author: aeril
"""

# scrap from shareASale

from ftplib import FTP 
import pandas as pd
import numpy as np
import errno
import os
import codecs
import json
import csv
import glob

#mylocal path
path='where you want to save the files'

def ftp_shareasale_download():
    def downloadfile(file_name):
        ftp.retrbinary('RETR '+file_name, open(file_name,'wb').write)
    # connect to ftp
    ftp = FTP('datafeeds.shareasale.com')
    ftp.login('username','password')
    ftp_list = ftp.nlst()
    
    alist=[]
    blist=[]
    #get all folders bh
 
    for file in ftp_list:
        if file.endswith(".txt"):
            blist.append(file)
        else:
            alist.append(file)
    
    for merchant in alist:
        if merchant.isdigit():
            eachpath='/'+merchant+'/'
            for each in ftp.cwd(eachpath):
                downloadfile(merchant+'.txt')
               
    ftp.quit()              
    
ftp_shareasale_download()

#DATA cleansing
def clean(product):
    #combine colunms after description
    for column in product.iloc[:,12:len(product)]:
        product[11]+=product[column].astype(str)
        
    d=product.drop(product.iloc[:,13:len(product)].columns, axis=1)
    d['category']=product[9].str.cat(product[10],sep='1908418')
    d['source']='shareAsale'
    d.columns=["product_id","product_name","brand_id","brand","product_link","product_image",
                     "big_img","price","retailer_price","m_category","subcategory","description","merch","category","source"]
    d['product_link'].replace('YOURUSERID','1908418')
    d['product_link'] = set(map(lambda x: x.replace("YOURUSERID","1908418"), d['product_link']))
    res=d.drop(['product_id','big_img','m_category','subcategory'],axis=1)
    
    res['color']=np.nan
    res['material']=np.nan
    res['size']=np.nan
    res.dropna(axis=1, how='all')
    return res


#process txt file
#on my local 

filelist=[]

for files in os.listdir(path):
    if files.endswith('.txt'):
        filelist.append(files)

csvlist=[]
for i in filelist:
    filepath=path+i
    with codecs.open(filepath, 'r' , encoding='utf-8', errors='ignore') as f:
        data=pd.read_csv(f, sep="|", header=None)
        test=clean(data)
        test.to_csv(path+i.replace(".txt",".csv"))
        csvlist.append(test)

#convert to json

for file in glob.glob('path/*.csv'):
    csvf=os.path.splitext(file)[0]
    jsonfile= csvf +'.json'
    
    with open (csvf +'.csv') as f:
        reader=csv.DictReader(f)
        rows = list(reader)
        
    with open (jsonfile, 'w') as f:
        json.dump(rows, f, indent=4, sort_keys=True)

#delete txt and csv file
filelist = [ f for f in os.listdir(path) if f.endswith(".csv") or f.endswith('.txt')]
for f in filelist:
    os.remove(os.path.join(path, f))
