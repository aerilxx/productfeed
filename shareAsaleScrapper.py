#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Nov 12 13:32:15 2018

@author: aeril
"""

# scrap from shareASale

from ftplib import FTP 
import errno
import os
import codecs
import json
import csv

#downloading
def ftp_download():
	def downloadfile(file_name):
	    print("Downloading ", file_name)
	    ftp.retrbinary('RETR '+file_name, open(file_name,'wb').write)

	def convert(file_name):
	    with codecs.open(file_name, 'r' , encoding='utf-8', errors='ignore') as f:
	        data = pd.read_csv(f, sep="|", header=None)
	        new_file = file_name.replace(".txt",".csv")
	        data.to_csv(new_file)

	    return new_file

	# connect to ftp
	ftp = FTP('datafeeds.shareasale.com')
	ftp.login('username','password')
	ftp_list = ftp.nlst()
	csv_file_list = []

	for f in ftp_list:
		if f.isdigit():
			ftp.cwd(f)
			for file in ftp.nlst():
				if file.endswith(".txt"):
					downloadfile(file)
					csv_file_list.append(convert(file))
					os.remove(file)
			ftp.cwd('../')
     
	return csv_file_list           
    
def saveToJson(product):
    with open('products.json', 'a') as f:
        json.dump(product, f, indent=4, sort_keys=True)
    print('save to json')
    
file_list=ftp_download()

product_dict_list = []       

#DATA cleansing
def clean():  
    
    for file in file_list:
    	with open(file) as csvfile:
            reader = csv.DictReader(csvfile)
       
            for row in reader:
                for col in range(12, 50):
                    row['11']+=row[str(col)]
                row['4']=row['4'].replace("YOURUSERID","1908418"),
                product_dict={
                'type':'add',
                'id':row['0'],
                'fields':{
        	           'title': row['1'],
        	           'brand': row['3'],
        	           'product_url': row['4'],
        	           'img_url': row['5'],
        	           'price': row['7'],
                   'description': row['11'],
        	           'color': ' ',
        	           'material': ' ',
        	           'size': ' ',
        	           'merch_id': row['2'],
        	           'merchant_name': row['3'],
        	           'source': 'ShareASale'
            	           }
            	    	}
                product_dict_list.append(product_dict)
           
    return product_dict_list


'''        
# upload to couldsearch    
def upload_to_cloudsearch(products_json):
    # turn json to bytes
    products_bytes = products_json.encode('utf-8')
    # establish link and upload
    client = boto3.client('cloudsearchdomain',endpoint_url = 'http://doc-frenzysearch-g2cbqftgpmzftr7am5fvng5sz4.us-west-1.cloudsearch.amazonaws.com')
    response = client.upload_documents(
                documents= products_bytes,
                contentType='application/json')
'''        
    
#start to chuck file and upload
 
product_queue = collections.deque()
product_queue.extend(clean())

batch_size = 4000


while len(product_queue) > batch_size:
    print('queue length', len(product_queue))
    seq = [product_queue.popleft() for _ in range(batch_size)]
    products_json = json.dumps(seq)
    saveToJson(seq)
    #upload_to_cloudsearch(products_json)

# if the product queue still contains products, upload one more time
if len(product_queue) >= 0:
    print('the last batch')
    products_json = json.dumps(list(product_queue))
    saveToJson(list(product_queue))
    #upload_to_cloudsearch(products_json)
    

#delete everything local
filelist = [ f for f in os.listdir(path) if f.endswith(".csv") or f.endswith('.txt')]
for f in filelist:
    os.remove(os.path.join(path, f))        

