import xml.etree.ElementTree as et
import json
import os
import sys
# download file from ftp and preprocessing
from ftplib import FTP
import gzip
import shutil
import re
import boto3
from celery import Celery

app = Celery('rakuten_tasks', broker='pyamqp://localhost')

@app.task
# take json str (not file) as input, turn it into bytes and upload it to cloudsearch
def upload(chuck):
    products_bytes = chuck.encode('utf-8')
    client = boto3.client('cloudsearchdomain',endpoint_url = 'http://doc-test-frenzy-search-bjus6b5jv5bmi3cnrb3fqz2jc4.us-west-1.cloudsearch.amazonaws.com')
    response = client.upload_documents(documents= products_bytes,contentType='application/json')
    print("uploading.....")
    return str(response['status'])
    

@app.task
# return a list of xml files
def download(file):

# download file from ftp to local machine,it returns a list of file name of the downloaded file
    def downloadfile(file_name):
    	ftp = FTP('aftp.linksynergy.com')
    	ftp.login('frenzylabs','V9P26CQ')
    	ftp.retrbinary('RETR '+file_name, open(file_name,'wb').write)

    ##unzip gz files
    def unzipfile(file_name):
    	new_name = file_name.replace(".gz","")
    	with gzip.open(file_name, 'rb') as f_in:
            with open(new_name, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
    	
    	return new_name
    	
    # read xml 
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

    def clean(products, merch_id, merch_name):

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


    #start main function
    downloadfile(file)
    uf=unzipfile(file)
    products, merch_id, merch_name = get_merch_products(uf)

    # convert to json
    with open(str(file) +'.json', 'a') as f:
        json.dump(clean(products, merch_id, merch_name), f, indent=4, sort_keys=True)

    os.remove(file)

    print ("sucessfully download file: " + file)

    return str(file) +'.json'









