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
import gzip
import shutil
import re
#sftp protocal
import paramiko

# return a list of product => products, the merch_id and the merch_name
# product are Element tree, element objects
def get_merch_products(file_name):
    tree = et.parse(file_name)
    root = tree.getroot()
    #merch_id = root.find('feed/entry/id').text
    #merch_name = root.find('header/merchantName').text
    products = root.findall('product')
    # products is a list of ElementTree element
    return products#, merch_id, merch_name

# function that return a list of dictionary
# dictionary are products data in json format
def product_processing(products):

    def add(product):
        name = product.find('name').text
        brand = product.find('manufacturer')
        buyurl = product.find('buyurl').text
        imageurl = product.find('imageurl').text
        price = product.find('price').text
        description = product.find('description').text.replace('&amp;','&')
        color = product.find('promotionaltext')
        merchant = product.find('programname').text.replace('&amp;','&')

        if (brand != None) and (brand.text != None):
            brand = brand.text
        else:
            brand = ''

        if (color != None) and (color.text != None):
            color = color.text
        else:
            color = ''

        product_dict = {
        'type': 'add',
        'fields':{
            'title': name,
            'brand': brand,
            'product_url': buyurl,
            'img_url': imageurl,
            'price': price,
            'description': description,
            'category': '',
            'color': color,
            'material': '',
            'size': '',
            'merchant_name':  merchant,
            'source': 'CJ'
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


# this function download the necessary data file from sftp to local machine
# it returns a list of file name of the downloaded file for future operation
def ftp_download():
    ###download files
    def downloadfile(file_name):
        print("Downloading ", file_name)
        sftp.get(file_name, file_name)
    ##unzip gz files
    def unzipfile(file_name):
        print('Unziping ', file_name)
        new_name = file_name.replace(".gz","")
        with gzip.open(file_name, 'rb') as f_in:
            with open(new_name, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    try:
        ssh.connect(hostname="datatransfer.cj.com",username="username",password="password")
    except paramiko.SSHException:
            print("Connection Failed")
            quit()

    sftp = ssh.open_sftp()
    sftp.chdir("/outgoing/productcatalog/216911/")

    file_list = sftp.listdir() 

    for file_name in file_list:
        downloadfile(file_name)
        unzipfile(file_name)
        os.remove(file_name)

    for i in range(len(file_list)):
        file_list[i] = file_list[i].replace('.gz','')

    # Closes the connection
    ssh.close()

    return file_list
