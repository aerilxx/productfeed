# this is a program that generate dictionary from raw xml ==> xmllint
# haven't studied it yet

# parse the data and turn it into json string
from lxml import etree as et
import json
# connect the program to cloudsearch
import boto3
# local file operation
import os
import sys
# create deque for file and data records
import collections
# download file from ftp and preprocessing
from ftplib import FTP
import gzip
import shutil
import re

import pysftp
import mysql.connector

# return a list of product => products, the merch_id and the merch_name
# product are Element tree, element objects
def get_merch_products(file_name):
    tree = et.parse(file_name)
    root = tree.getroot()
    #merch_id = root.find('feed/entry/id').text
    #merch_name = root.find('header/merchantName').text
    products = root.findall('entry')
    # products is a list of ElementTree element
    return products#, merch_id, merch_name

# function that return a list of dictionary
# dictionary are products data in json format
def product_processing(products):

    def add(product):
        product_id = product.find('id').text
        product_name = product.find('title').text
        product_link = product.find('link').text.replace('&amp;','&')
        product_image = product.find('image_link').text.replace('&amp;','&')

        if (product.find('description') != None) and (product.find('description').text != None):
            description_long = product.find('description').text
        else:
            description_long = ''

        if product.find('sale_price') != None and product.find('sale_price').text != None:
            price = product.find('sale_price').text.replace(' USD','')
        elif product.find('price') != None and product.find('price').text != None:
            price = product.find('price').text.replace(' USD','')
        else:
            price = ''

        if product.find('brand') != None and product.find('brand').text != None:
            brand = product.find('brand').text
        else:
            brand = ''

        if product.find('product_type') != None and product.find('product_type').text != None:
            category = product.find('product_type').text
        elif product.find('google_product_category_name') != None and product.find('google_product_category_name').text != None:
            category = product.find('google_product_category_name').text
        else:
            category = ''
        ###   text = et.tostring(product,encoding = 'unicode')  ###

        if product.find('color') != None and product.find('color').text != None:
            color = product.find('color').text
        else:
            color = ''

        if product.find('material') != None and product.find('material').text != None:
            material = product.find('material').text
        else:
            material = ''

        product_dict = {
        'type': 'add',
        'id':product_id,
        'fields':{
            'product_name':product_name,
            'brand': brand,
            'category':category,
            'product_link':product_link,
            'product_image':product_image,
            'price': price,
            'description_long': description_long,
            'color': color,
            'material': material,
            }
        }

        return product_dict

    product_dict_list = [0]*len(products)
    i = 0
    # we may need to divide products into small segment and process one segment
    # at a time to make sure the result file in within 5 MB;
    for product in products:
        product_dict = add(product)
        product_dict_list[i] = product_dict
        i = i + 1

    return product_dict_list

# take json str as input, turn it into bytes and upload it to cloudsearch
def upload_to_cloudsearch(products_json):
    # turn json to bytes
    products_bytes = products_json.encode('utf-8')
    # establish link and upload
    with open('checkresult.json','wb') as fh:
        fh.write(products_bytes)
    client = boto3.client('cloudsearchdomain',endpoint_url = 'http://doc-frenzysearch-g2cbqftgpmzftr7am5fvng5sz4.us-west-1.cloudsearch.amazonaws.com')
    response = client.upload_documents(
                documents= products_bytes,
                contentType='application/json')

# function that popluate the product_queue
# when the products it contains is less that 3500 records
def populate_product_queue(file_queue):
    file_name = file_queue.popleft()
    print('processing ', file_name)
    products = get_merch_products(file_name)
    i = 0
    for k in products:
        i +=1
    print(i)
    product_queue.extend(product_processing(products))
    #os.remove(file_name)

# files ==> a file name list
# file_list ==> a list of file name that we need to download and process
# It go through all the file in ftp, get the merch_id from the file name, search on cloudsearch.
# If the merch_id don't exist in cloudsearch, put the full catelog file's name in the file_list (one that has "mp.xml")
# We put all the delta file (ones that has "delta.xml") in the file_list
def create_file_list(files):
    file_list = []
    for x in files:
        if 'mp.xml' in x:
            merch_id = re.search(r'([0-9]*)_',x).group(1)
            # do cloudsearch
            client = boto3.client('cloudsearchdomain', endpoint_url = 'http://search-frenzysearch-g2cbqftgpmzftr7am5fvng5sz4.us-west-1.cloudsearch.amazonaws.com')
            response = client.search(
                query='merch_id:'+ str(merch_id),
                queryParser='lucene',
                size=10)
            if response['hits']['found'] == 0:
                file_list.append(x)
        if 'delta.xml' in x:
            file_list.append(x)
    return file_list

# this function download the necessary data file from ftp to local machine
# it returns a list of file name of the downloaded file for future operation
def ftp_download():
    ###download files
    def downloadfile(file_name):
        print("Downloading ", file_name)
        ftp.retrbinary('RETR '+file_name, open(file_name,'wb').write)
    ##unzip gz files
    def unzipfile(file_name):
        print('Unziping ', file_name)
        new_name = file_name.replace(".gz","")
        with gzip.open(file_name, 'rb') as f_in:
            with open(new_name, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)

    # connect to ftp
    ftp = FTP('aftp.linksynergy.com')
    ftp.login('frenzylabs','V9P26CQ')
    ftp_list = ftp.nlst()

    # ftp_list = ['42030_3529748_mp_delta.xml.gz','42030_3529748_mp.xml.gz','41610_3529748_mp_delta.xml.gz','2311_3529748_mp.xml.gz','2311_3529748_mp_delta.xml.gz']
    file_list = create_file_list(ftp_list)
    for file_name in file_list:
        downloadfile(file_name)
        unzipfile(file_name)
        os.remove(file_name)

    for i in range(len(file_list)):
        file_list[i] = file_list[i].replace('.gz','')

    return file_list


# start of the main function
# file_list is a list of file name we will use in to following processing step

# file_list = ftp_download()
# print(file_list)

# this is for testing purpose, just change the file_list
file_list = ['STYLEBOP_com_US_Canada-US_Google_Shopping-shopping.xml']

# this part go through the products in files, put them in a queue and
# upload 4000 products a batch (since the cloudsearch has a limit upload filesize of 5MB per batch)

file_queue = collections.deque()
product_queue = collections.deque()
file_queue.extend(file_list)

batch_size = 4000
while len(file_queue) >= 0:
    # while the product queue is larger than the batch size
    while len(product_queue) > batch_size:
        print('quene length', len(product_queue))
        seq = [product_queue.popleft() for _ in range(batch_size)]
        products_json = json.dumps(seq)
        upload_to_cloudsearch(products_json)

    # if the product queue is smaller than the batch size
    # if there is no file left, simple upload what's left in the queue
    if len(file_queue) == 0:
        print('the last batch')
        products_json = json.dumps(list(product_queue))
        upload_to_cloudsearch(products_json)
        break

    # if the queue size is less that 4000, read another file and add it to the queue
    else:
        populate_product_queue(file_queue)



# chunk = products_chunks[0]
# products_json = chunk_processing(chunk, merch_id, merch_name)
# products_bytes = json_to_bytes(products_json)
# print(sys.getsizeof(products_bytes))

# test to use pyshtp to connect cj shtp
srv = pysftp.Connection(host="your_FTP_server", username="megan@frenzy.ai",
password="Frenzy2018!")

# Get the directory and file listing
data = srv.listdir()




# dynamic connect to myphpadmin to grab userID as SID 




# first, use local file to test
# use boto to upload the data
# seperate the document to 5MB batches
# combine small files to 5MB batches
# integrate the download, unzip, delete file function

# **********

# start an instance on EC2, install all necessary python package and run the program on EC2

# build API

# client = boto3.client('cloudsearchdomain')
# response = client.upload_documents(documents = b'${products_json}', contentType = 'application/json')