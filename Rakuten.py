# this is a program that generate dictionary from raw xml ==> xmllint
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
# return a list of product => products, the merch_id and the merch_name
# product are Element tree, element objects
def get_merch_products(file_name):
    tree = et.parse(file_name)
    root = tree.getroot()
    merch_id = root.find('header/merchantId').text
    merch_name = root.find('header/merchantName').text
    products = root.findall('product')
    products_in_stock = []
    for item in products:
        if item.find('shipping/availability')!=None and item.find('shipping/availability').text == "in-stock":
            products_in_stock.append(item)
    return products_in_stock, merch_id, merch_name

# function that return a list of dictionary
# dictionary are products data in json format
def product_processing(products, merch_id, merch_name):

    def delete(product):
        product_id = product.get('product_id')
        product_dict = {
        'type':'delete',
        'id':product_id
        }
        return product_dict

    def add(product):
        product_id = product.get('product_id')
        product_name = product.get('name')
        product_link = product.find('URL/product').text.replace('&amp;','&')
        product_image = product.find('URL/productImage').text.replace('&amp;','&')

        if (product.find('description/short') != None) and (product.find('description/short').text != None):
            description_short = product.find('description/short').text
        else:
            description_short = ''

        if product.find('description/long') != None and product.find('description/long').text != None:
            description_long = product.find('description/long').text
        else:
            description_long = ''

        if product.find('price/sale') != None and product.find('price/sale').text != None:
            price = product.find('price/sale').text
        elif product.find('price/retail') != None and product.find('price/retail').text != None:
            price = product.find('price/retail').text
        else:
            price = ''

        if product.find('brand') != None and product.find('brand').text != None:
            brand = product.find('brand').text
        elif product.get('manufacturer_name') != None :
            brand = product.get('manufacturer_name')
        else:
            brand = ''

        if product.find('attributeClass/Product_Type') != None and product.find('attributeClass/Product_Type').text != None:
            category = product.find('attributeClass/Product_Type').text
        elif product.find('category/secondary') != None and product.find('category/secondary').text != None:
            category = product.find('category/secondary').text
        elif product.find('category/primary') != None and product.find('category/primary').text != None:
            category = product.find('category/primary').text
        else:
            category = ''
        ###   text = et.tostring(product,encoding = 'unicode')  ###

        if product.find('attributeClass/Color') != None and product.find('attributeClass/Color').text != None:
            color = product.find('attributeClass/Color').text
        else:
            color = ''

        if product.find('attributeClass/Material') != None and product.find('attributeClass/Material').text != None:
            material = product.find('attributeClass/Material').text
        else:
            material = ''

        product_dict = {
        'type': 'add',
        'id':product_id,
        'fields':{
            'title': product_name,
            'brand': brand,
            'product_url': product_link,
            'img_url': product_image,
            'price': price,
            'description': description_long,
            'category': category,
            'color': color,
            'material': material,
            'size': '',
            'merch_id': merch_id,
            'merchant_name': merch_name,
            'source': 'Rakuten'
            }
        }

        return product_dict


    product_dict_list = [0]*len(products)
    i = 0
    # we may need to divide products into small segment and process one segment
    # at a time to make sure the result file in within 5 MB;
    for product in products:

        if product.find('modification') != None:
            # If the product is to be deleted
            if product.find('modification').text == 'D':
                product_dict = delete(product)
            else:
                product_dict = add(product)
        # for new product or update product
        else:
            product_dict = add(product)


        product_dict_list[i] = product_dict
        i = i + 1

    return product_dict_list

# function that popluate the product_queue
# when the products it contains is less that 3500 records
def populate_product_queue(file_queue):
    file_name = file_queue.popleft()
    print('processing ', file_name)
    products, merch_id, merch_name = get_merch_products(file_name)
    print(merch_id, merch_name)
    product_queue.extend(product_processing(products, merch_id, merch_name))
    # os.remove(file_name)


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
    ftp.login('username','password')
    ftp_list = ftp.nlst()

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

# this part go through the products in files, put them in a queue and
# upload 4000 products a batch (since the cloudsearch has a limit upload filesize of 5MB per batch)

file_queue = collections.deque()
product_queue = collections.deque()
file_queue.extend(file_list)

batch_size = 4000
new_json_file = open('data.json','w')
while len(file_queue) >= 0:
    # while the product queue is larger than the batch size
    while len(product_queue) > batch_size:
        print('quene length', len(product_queue))
        products_json = json.dump([product_queue.popleft() for _ in range(batch_size)],new_json_file)
        # upload_to_cloudsearch(products_json)

    # if the product queue is smaller than the batch size
    # if there is no file left, simple upload what's left in the queue
    if len(file_queue) == 0:
        print('the last batch')
        products_json = json.dump(list(product_queue),new_json_file)
        # upload_to_cloudsearch(products_json)
        new_json_file.close()
        break

    # if the queue size is less that 4000, read another file and add it to the queue
    else:
        populate_product_queue(file_queue)


