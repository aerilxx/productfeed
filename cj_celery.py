from celery import Celery
import boto3
import json
import os
import pysftp as sftps
# download file from ftp and preprocessing
import gzip
import shutil
from botocore.client import Config
import paramiko
import xml.etree.ElementTree as et

app = Celery('cj_tasks', broker='pyamqp://localhost')
    

@app.task
# return a list of xml files
def download(file):

    def downloadfile(file_name):
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try:
            ssh.connect(hostname="datatransfer.cj.com",username="5102504",password="v7~MXEym")
        except paramiko.SSHException:
            print("Connection Failed")
            quit()

        sftp = ssh.open_sftp()
        sftp.chdir("/outgoing/productcatalog/217585/")
        sftp.get(file_name, file_name,callback=None)

    ##unzip gz files
    def unzipfile(file_name):
        new_name = file_name.replace(".xml.gz",".xml")
        with gzip.open(file_name, 'rb') as f_in:
            with open(new_name, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)

        return new_name
    

    # function that return a list of dictionary
    # dictionary are products data in json format
    def clean(products):
        def add(product):
            name = product.find('name').text
            product_id = product.find('sku').text
            brand = product.find('manufacturer')
            buyurl = product.find('buyurl')
            imageurl = product.find('imageurl')
            price = product.find('price')
            description = product.find('description').text.replace('&amp;','&')
            color = product.find('promotionaltext')
            merchant = product.find('programname').text.replace('&amp;','&')

            if (brand != None) and (brand.text != None):
                brand = brand.text
            else:
                brand = ''

            if (buyurl != None) and (buyurl.text != None):
                buyurl = buyurl.text
            else:
                buyurl = ''

            if (imageurl != None) and (imageurl.text != None):
                imageurl = imageurl.text
            else:
                imageurl = ''

            if (price != None) and (price.text != None):
                price = price.text
            else:
                price = ''

            if (color != None) and (color.text != None):
                color = color.text
            else:
                color = ''

            product_dict = {
            'type': 'add',
            'id': product_id,
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
                'merch_id': merchant,
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

 # read xml
    def get_merch_products(xmlfile):
        e = et.parse(xmlfile)
        root = e.getroot()
        products = root.findall('product')
        return products

#start main function
    downloadfile(file)
    f=unzipfile(file)
    product= get_merch_products(f)

# convert to json
    with open(str(file) +'.json', 'a') as f:
        json.dump(clean(product), f, indent=4, sort_keys=True)

    os.remove(file)

    return "sucessfully download file: " + file



