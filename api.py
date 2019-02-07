#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jan 16 17:14:20 2019

@author: bingqingxie
"""

import Flask
from flask import request, jsonify, send_file
import boto3
import json
import os 


#app configuration 

app = Flask(__name__)
app.config["DEBUG"] = True
app.config.from_object(__name__)
app.config['SECRET_KEY'] = '****'

#app configuration 

app = Flask(__name__)
app.config["DEBUG"] = True
app.config.from_object(__name__)
app.config['SECRET_KEY'] = 'frenzy2018datafeedsearchengine'

def Wrap(feed, url, subid):
	new_link = ' '
	if feed == 'Rakuten':
		if str(url).startswith(''):
			new_link = 'http://click.linksynergy.com/link?' + url + '&u1=' + subid
		if str(url).startswith('http'):
			new_link = url + '&u1=' + subid

	elif feed == 'cj':
		if str(url).startswith('url'):
			new_link = 'http://www.dpbolvw.net/click-8810497-11192081?sid='+ subid + '&' + url 
		if str(url).startswith('http://'):
			new_link = url[0:url.find('?')]+'?sid='+ subid + '&' + url[url.find('?')+1:]

	elif feed == 'Pepperjam':
		if str(url).startswith('url'):
			new_link = '	http://www.pjtra.com/t/Qz9JSkJGP0NKRURGRT9JSkJG?sid='+ subid + '&' + url 
		if str(url).startswith('http://'):
			new_link = url[0: url.find('?')]+'?sid='+ subid + '&' + url[url.find('?')+1:]

	elif feed == 'ShareASale':
		new_link = url + 'afftrack=' + subid

	return new_link



@app.route('/', methods=['GET'])
def home():
    return "<h1>This is Frenzy Search: </h1><p>This site is a prototype API for search products according to key word.</p>"

@app.route('/products/<brand>/<category>/<subid>', methods=['GET'])
def searchproductsbybrand(brand, category, subid):
    
    client = boto3.client('cloudsearchdomain',
	endpoint_url = '  ')
	###  change request content
    query = "(and brand:'brandname' category:'categoryname')"
    query = query.replace("brandname",brand).replace("categoryname", category)
	#####  change search paremeters
    response = client.search(query=query,
		    queryParser='structured',
		    returnFields="brand,category,title,product_url,img_url,description,color,material,merch_id,merchant_name,price,size,source",
		    size=10000) #maxiamum size is 10,000
  
    for i in response['hits']['hit']:
        i['fields']['product_url'][0]= Wrap( i['fields']['source'][0] , i['fields']['product_url'][0], subid)
    
    result = response['hits']['hit']

    if response['hits']['hit']:
        return jsonify(result), 200
    
    return "product not found", 404


@app.route('/products/<color>/<category>/<subid>', methods=['GET'])
def searchproductsbycolor(color, category, subid):
    
    client = boto3.client('cloudsearchdomain',
	endpoint_url = '   ')
	###  change request content
    query = "(and color:'color' category:'categoryname')"
    query = query.replace("color",color).replace("categoryname", category)
	#####  change search paremeters
    response = client.search(query=query,
		    queryParser='structured',
		    returnFields="brand,category,title,product_url,img_url,description,color,material,merch_id,merchant_name,price,size,source",
		    size=10000) #maxiamum size is 10,000
  
    for i in response['hits']['hit']:
        i['fields']['product_url'][0]= Wrap( i['fields']['source'][0] , i['fields']['product_url'][0], subid)
    
    result = response['hits']['hit']

    if response['hits']['hit']:
        return jsonify(result), 200
    
    return "product not found", 404


@app.route('/products/<description>/<subid>', methods=['GET'])
def searchproductsbydescription(description, subid):
    
    client = boto3.client('cloudsearchdomain',
	endpoint_url = '  ')
    response = client.search(query="(and description: '" + str(description)+"')",
		    queryParser='structured',
		    returnFields="brand,category,title,product_url,img_url,description,color,material,merch_id,merchant_name,price,size,source",
		    size=10000) #maxiamum size is 10,000
  
    for i in response['hits']['hit']:
        i['fields']['product_url'][0]= Wrap( i['fields']['source'][0] , i['fields']['product_url'][0], subid)
    
    result = response['hits']['hit']

    if response['hits']['hit']:
        return jsonify(result), 200
    
    return "product not found", 404

#<string:category><string:brand><string:color><string:description><int:subid>
@app.route('/products/mysearch', methods=['GET'])
def searchindetail():
    category = request.args.get('category')
    brand = request.args.get('brand')
    color = request.args.get('color')
    description = request.args.get('desc')
    subid = request.args.get('subid')
    print(category,brand,color,description,subid)
    
    client = boto3.client('cloudsearchdomain',
	endpoint_url = ' ')
	###  change request content
    query = "(and color: '"+color+ "' category: '"+ category+ "' brand: '"+brand + "' description: '"+description+"')"
    
    print(query)
    #####  change search paremeters
    response = client.search(query=query,
		    queryParser='structured',
		    returnFields="brand,category,title,product_url,img_url,description,color,material,merch_id,merchant_name,price,size,source",
		    size=10000) #maxiamum size is 10,000
    
    for i in response['hits']['hit']:
        i['fields']['product_url'][0]= Wrap( i['fields']['source'][0] , i['fields']['product_url'][0], subid)
    
    result = response['hits']['hit']

    if response['hits']['hit']:
        return jsonify(result), 200
    
    return "product not found", 404
    

if __name__ == '__main__':
     app.run(port='****')





