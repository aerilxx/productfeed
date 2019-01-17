#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jan  3 16:51:10 2019

@author: bingqingxie
"""

import requests
import json
import boto3
import re
import uuid
from multiprocessing.pool import ThreadPool
from botocore.client import Config

perpperjam_api='private_api'

def getRequest(resource, params):
    #print ('downloading '+str(resource))
    url = 'https://api.pepperjamnetwork.com/20120402/'+resource+'?apiKey='+perpperjam_api
    for param in params:
        url += '&'+param+'='+params[param]
      
    r = requests.get(url)
    data = json.loads(r.text)
    return data

# retrieve joined advertisers ID
def getAdvertisers():    
    advertiser_ids = []
    advertisers = getRequest('publisher/advertiser',{'format':'json','status':'joined'})
    brand_id_pair = advertisers['data']
    for advertiser in brand_id_pair:
        advertiser_ids.append(advertiser['id'])
        
    return advertiser_ids

def clean(products):    
    product_dict_list = []
    for product in products:
        # provent product_id upload error
         pattern = re.compile("[a-zA-Z0-9\-\_\/\#\:\.\;\&\=\?\@\$\+\!\*'\(\)\,\%]+$")
         if pattern.match(product['sku']):
            product_id = product['sku']
         else:
            product_id = str(uuid.uuid4())
        
         product_dict = {
        'type': 'add',
        'id': product_id, 
        'fields':{
            'title': product['name'],
            'brand': product['manufacturer'],
            'product_url': product['buy_url'],
            'img_url': product['image_url'],
            'price': product['price'],
            'description': product['description_long'],
            'category': product['category_program'],
            'color': product['color'],
            'material': product['material'],
            'size': product['size'],
            'merch_id': product['program_id'],
            'merchant_name': product['program_name'],
            'source': 'Pepperjam'
            }
        }
         product_dict_list.append(product_dict)
    
    return product_dict_list


def getProductsByPage(program,pageNo):
	products = getRequest('publisher/creative/product',{'format':'json','programIds':program,'page':pageNo})
	product_array = products['data']
	product_dict_list = clean(product_array)

	return product_dict_list

#retrieve all product information based for a speicifc program
def getProducts(brands):
    product_dict_list=[]
    for brand in brands :
        #get request must be in str not int
        products = getRequest('publisher/creative/product',{'format':'json','programIds': str(brand)})
     
        if 'pagination' in products['meta']:
            total_pages = products['meta']['pagination']['total_pages'] 
            print (total_pages)
            product_array = products['data']
            product_dict_list = clean(product_array)
            for page in range(1,total_pages):
                    product_dict_list += getProductsByPage(brand, str(page))    
        else:
            print (products['meta']['status']['message'])
    
        print (brand+' downloading ...')
    return product_dict_list


# upload to couldsearch    
def upload_to_cloudsearch(products_json):
    # turn json to bytes 
    products_bytes = products_json.encode('utf-8').strip()
    # establish link and upload
    client = boto3.client('cloudsearchdomain',endpoint_url = 'aws)
    client.upload_documents(documents= products_bytes,contentType='application/json')
 

def saveToJson(product):
    with open('pepperjam_products.json', 'a') as f:
	    json.dump(product, f, indent=4, sort_keys=True)   
    print('saved to json')	

def upload_test (p_list):
    products_json = json.dumps(p_list)
    saveToJson(p_list)
    upload_to_cloudsearch(products_json)
    print ('uploading')
    
# Yield successive n-sized 
# chunks from l. 
def divide_chunks(l, n): 
    # looping till length l 
    for i in range(0, len(l), n):  
        yield l[i:i + n] 
        
if __name__ == '__main__': 
#first run
    all_products=getAdvertisers()

    product_list=[]
#use extend instead of append to get all
    product_list.extend(getProducts(all_products))
    
    # using list comprehension, devide all products into 4000 each
    n = 4000
    final = divide_chunks(product_list, n)
    
# set retries to avoid timeout 
    config = Config(connect_timeout=10, retries={'max_attempts': 0})
    pool= ThreadPool(processes=3)
    pool.map(upload_test, final)
    print('done')
        
        
