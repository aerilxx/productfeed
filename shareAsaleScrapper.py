#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Nov 12 13:32:15 2018

@author: aeril
"""

# scrap from shareASale

from ftplib import FTP
import pandas as pd
import os
import codecs
import json
import csv
import collections
import boto3

#mylocal path
path='/Users/bingqingxie/Desktop/Desktop - Bingqing’s MacBook Air/source feed/'


#downloading all files, return a list of csv file
def ftp_download(ftp):
    
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
	ftp_list = ftp.nlst()
	csv_file_list = []

	for f in ftp_list:
		if f.isdigit():
			ftp.cwd(f)
			for file in ftp.nlst():
				if file.endswith(".txt"):
					downloadfile(file)
					csv_file_list.append(convert(file))
					#os.remove(file)
			ftp.cwd('../')
     
	return csv_file_list           
    

# save file as json format
def saveToJson(product):
    with open('shareasale_products.json', 'a') as f:
        json.dump(product, f, indent=4, sort_keys=True)
    print('save to json')
    

#DATA cleansing, return a list of products
def clean (file_list):
    product_dict_list=[]
    for file in file_list:
        with open(file) as csvfile:
            reader = csv.DictReader(csvfile)
           
            for row in reader:
                for col in range(12, 50):
                    row['11']+=row[str(col)] 
                
                product_dict={
                'type':'add',
                'id':row['0'],
                'fields':{
        	           'title': row['1'],
        	           'brand': row['3'],
        	           'product_url': row['4'].replace("YOURUSERID","1908418"),
        	           'img_url': row['5'],
        	           'price': row['7'],
        	           'category': row['10']+row['9'],
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

#compare with ftp to grab the changed file
   #return a list of new csv file
def getUpdateFile(file_list, ftp):
    file_list_update = []
    
    def downloadfile(file_name):
	    print("Downloading ", file_name)
	    ftp.retrbinary('RETR '+file_name, open(file_name,'wb').write)
        
    def convert(file_name):
        
	    with codecs.open(file_name, 'r' , encoding='utf-8', errors='ignore') as f:
	        data = pd.read_csv(f, sep="|", header=None, low_memory=False)
	        new_file = file_name.replace(".txt","new.csv")
	        data.to_csv(new_file)
        
	    return new_file

    entries = list(ftp.mlsd())
    entries.sort(key = lambda entry: entry[1]['modify'], reverse = True)
    updatefile = entries[0][0]
    downloadfile(updatefile)
    print(updatefile)

    for file in file_list:
        if file[0:5] in open(updatefile).read():
            changed_file_name=[]
            changed_file_name.append(file.replace(".csv", ""))
  
            for f in changed_file_name:
                ftp.cwd(f)
                for file in ftp.nlst():
                    if file.endswith(".txt"):
                        downloadfile(file)
                        file_list_update.append(convert(file))
                ftp.cwd('../')
                    
            print('have to update '+ file)

        else:
           print(file+' is not changed')     
        
    os.remove(updatefile)     
    return file_list_update

#compare with ftp to add new merchertise
def addNewFile(file_list, ftp):
    file_list_add = []
    
    for file in file_list:
        if file.replace(".csv","") in ftp.nlst():
            print(file + " already exist")
        else:
            file_list_add.append(file)		
    
    return file_list_add
    

#function that creates a map of product
# key: product_id .... value: [name, buyurl, imageurl, price, description]
def createMap(file):
    product_map = {}

    with open(file) as csvfile:
        reader = csv.DictReader(csvfile)
       
        for row in reader:
            product_id=row['0']
            name=row['1']
            merch_id = row['2']
            merchant_name = row['3']
            buyurl=row['4'].replace("YOURUSERID","1908418")
            imageurl=row['5']
            price=row['7']
            category=row['9']+row['10']
            description = ''
            for i in range(12,50):
                description +=" "+row[str(i)]
            
            product_map[product_id] = [name, buyurl, imageurl, price, category, description, merch_id, merchant_name]

    #print(len(product_map))
    return product_map

#compare csv file to find difference 
def update(file, file_map):

	product_dict_list=[]

	def delete(pid):
		product_dict = {'type': 'delete','id': pid } 
		return product_dict

	def add(pid):
		product_dict={
                'type':'add',
                'id':pid,
                'fields':{
        	           'title': row['1'],
        	           'brand': row['3'],
        	           'product_url': row['4'].replace("YOURUSERID","1908418"),
        	           'img_url': row['5'],
        	           'price': row['7'],
        	           'category': row['9']+row['10'],
                   'description': description, 
                   'color': ' ',
        	           'material': ' ',
        	           'size': ' ',
        	           'merch_id': row['2'],
        	           'merchant_name': row['3'],
        	           'source': 'ShareASale'
            	           }
            	   	}

		return product_dict

	with open(file) as csvfile:
		reader = csv.DictReader(csvfile)

		for row in reader:
			pid=row['0']
			description = ''
			for i in range(12,50):
				description += ' '+row[str(i)]
 # if the product is in the new map
			if pid in file_map:
# there is a change
				if row['1']!=file_map[pid][0] or row['4']!=file_map[pid][1] or row['5']!=file_map[pid][2] or row['7']!=file_map[pid][3] or row['11']!=file_map[pid][4]:
					product_dict_list.append(add(pid))
					del file_map[pid]	
				# the product is deleted
			else:
				product_dict_list.append(delete(pid))

			for file in file_map:
				product_dict_list.append(add(file))

			del file_map

		return product_dict_list


# upload to couldsearch    
def upload_to_cloudsearch(products_json):
    # turn json to bytes
    products_bytes = products_json.encode('utf-8').strip()
    # establish link and upload
    client = boto3.client('cloudsearchdomain',endpoint_url = 'http://doc-test-frenzy-search-bjus6b5jv5bmi3cnrb3fqz2jc4.us-west-1.cloudsearch.amazonaws.com')
    client.upload_documents(documents= products_bytes,contentType='application/json')
    print ("upload successful")
  
    
#start to chuck file and upload
def uploadfile(product_queue):
    batch_size = 4000
    while len(product_queue) > batch_size:
        print('queue length', len(product_queue))
        seq = [product_queue.popleft() for _ in range(batch_size)]
        products_json = json.dumps(seq)
        saveToJson(seq)
        upload_to_cloudsearch(products_json)
        print ("uploading")

    # if the product queue still contains products, upload one more time
    if len(product_queue) >= 0:
        print('the last batch')
        products_json = json.dumps(list(product_queue))
        saveToJson(list(product_queue))
        upload_to_cloudsearch(products_json)
        print ("the last batch uploaded")
    

#delete everything local
def deletelocal():
    filelist = [ f for f in os.listdir() if f.endswith('.txt')]
    for f in filelist:
        os.remove(f)  

 # startint point

if __name__ == '__main__':
    ftp = FTP('datafeeds.shareasale.com')
    ftp.login('frenzylabs','frenzy2018') 
    file_list=ftp_download(ftp)
    
    # run the first time

    product_queue = collections.deque()
    product= clean(file_list)
    product_queue.extend(product)
    # first time upload without any update check
    uploadfile(product_queue)

    # upload new file if exist
    file_list_add=addNewFile(file_list,ftp)
    if file_list_add:
    	product_queue_new = collections.deque()
    	product_queue_new.extend(clean(file_list_add))
    	uploadfile(product_queue_new)
     
    # updated local file, delete old file, rename new.csv to .csv
    new_file_list=getUpdateFile(file_list, ftp)
    product_queue_update = collections.deque()
    product_queue_update.extend(clean(new_file_list))
    uploadfile(product_queue_update) 
    for file in new_file_list:
        os.remove(file)
        os.rename(file, file.replace("new.csv",".csv"))
    	
    deletelocal()




