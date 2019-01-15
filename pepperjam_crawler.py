# -*- coding: utf-8 -*-


import requests
import json
import collections
import boto3
import re
import uuid

perpperjam_api='xxx'

def getRequest(resource, params):
    #print ('downloading '+str(resource))
    url = 'https://api.pepperjamnetwork.com/20120402/'+resource+'?apiKey='+perpperjam_api
    for param in params:
        url += '&'+param+'='+params[param]
      
    r = requests.get(url)
    data = json.loads(r.text)
    return data

# retrieve advertisers ID
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
    client = boto3.client('cloudsearchdomain',endpoint_url = 'http://doc-test-frenzy-search-bjus6b5jv5bmi3cnrb3fqz2jc4.us-west-1.cloudsearch.amazonaws.com')
    client.upload_documents(documents= products_bytes,contentType='application/json')
 

def saveToJson(product):
    with open('pepperjam_products.json', 'a') as f:
	    json.dump(product, f, indent=4, sort_keys=True)   
    print('saved to json')	
        

def upload(product_queue):
    batch_size=4000
    
    while len(product_queue) > batch_size:
        print('queue length', len(product_queue))
        seq = [product_queue.popleft() for _ in range(batch_size)]
        products_json = json.dumps(seq)
        saveToJson(seq)
        upload_to_cloudsearch(products_json)
        print ('uploading')

# if the product queue still contains products, upload one more time
    if len(product_queue) >= 0:
        print('the last batch')
        products_json = json.dumps(list(product_queue))
        saveToJson(list(product_queue))
        upload_to_cloudsearch(products_json)
        print ('the last batch uploadede')
    
#update
#get new joined merchadise 
def getNewAdvertiser():
    new=[]
    old_adv=['4595', '6152', '6291', '6301', '6309', '6819', '6955', '7047', '7081', '7297', '7363', '7372', '7386', '7472', '7559', '7582', '7716', '7726', '7753', '7800', '7804', '7908', '8059', '8064', '8106', '8153', '8159', '8187', '8190', '8222', '8279', '8342', '8344', '8345', '8367', '8370', '8381', '8427', '8464', '8466', '8521', '8539', '8585', '8595', '8631', '8654', '8666', '8680', '8714', '8743', '8827', '8835', '8859', '8881', '8975', '8988', '8994', '9026', '9027', '9031', '9039', '9062', '9089', '9091', '9094', '9126', '9141', '9145', '9179', '9180', '9190', '9231', '9236', '9239', '9242', '9256', '9257', '9273', '9301', '9312']
    new_adv=getAdvertisers()
    file = ''
    if file in new_adv and file not in old_adv:
        new.append(file)
    return new

'''
#compare new download products with cloudsearch products
def update(new_products, old_products):
    
    product_dict_list = []
    
    def delete(pid):     
        product_dict = {'type': 'delete', 'id': pid }
            
        return product_dict
    
    def add(pid):
        product_dict = {
            'type': 'add',
            'id': pid,
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
        
        return product_dict
    
    for product in new_products:
        pid=new_products['sku']
        client = boto3.client('cloudsearchdomain', endpoint_url = 'xxx.amazonaws.com')
        response = client.search(query='pid:'+ str(pid),
                                 queryParser='lucene',
                                 size=20)
        if response['hits']['found'] == 0:
            product_dict_list.append(add(pid))
        else:
            product_dict_list.append(delete(pid))
                   
    return product_dict_list
'''

   

if __name__ == '__main__': 
    #first run
    advertisers=getAdvertisers()

    product_queue = collections.deque()
    product_queue.extend(getProducts(advertisers))
    upload(product_queue)
    print ('pepperjam done...')
    '''
    #second run
    advertisers= getNewAdvertiser()
    product_queue = collections.deque()
    product_queue.extend(getProducts(advertisers))
    upload(product_queue)
    print ('pepperjam update done...')
    '''
        
        
