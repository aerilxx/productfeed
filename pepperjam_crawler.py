# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

from urllib.request import urlopen
import json


perpperjam_api='api'
resource='publisher/creative/product'

# retrieve all programID to find more products
url_getProgramId='https://api.pepperjamnetwork.com/20120402/publisher/advertiser?apiKey='+perpperjam_api+'&format=json&status=joined'


id_data=urlopen(url_getProgramId).read()
data=json.loads(id_data.decode('utf-8'))

brand_array=data['data']
range_data=len(brand_array)
dic_brandId={}
id_array=[]

for each_brand in brand_array:
    dic_brandId[each_brand['name']]=each_brand['id']
    id_array.append(each_brand['id'])
        
#print(dic_brandId)       

# get all products info from API according to programId
url_getProduct=[]
products_data_url=[]

for each_id in id_array:
    url_getProduct.append('https://api.pepperjamnetwork.com/20120402/'+resource+'?apiKey='+perpperjam_api+'&format=json'+'&programIds='+each_id)
    
for item in range(len(url_getProduct)):
    products_data_url.append(urlopen(url_getProduct[item],timeout=10).read())
    product_data=json.loads(products_data_url[item].decode('utf-8'))
    print(product_data)

page=len(url_getProduct)-1
print(url_getProduct[page])
#urlopen(url_getProduct[page],timeout=10).read()
#product_data=json.loads(products_data_url[page].decode('utf-8'))
#print(product_data)

#create category
"""
def add(product):
        product_name = product_data.get('name')
        product_link = product_data.get('buy_url')
        product_image = product_data.get('image_url')
        price=product_data.get('price')
        material=product_data.get('material')
        color=product_data.get('color')
        brand=product_data.get('manufacturer')
        merch_name=product_data.get('programe_name')
        merch_id=product_data.get('program_id')
        category=product_data.get('category_program')
        description_long=product_data.get('description_long')
        
        product_dict = {
        'type': 'add',
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
            'merch_id': merch_id,
            'merch_name':merch_name,
            }
        }
        
        return product_dict
"""

