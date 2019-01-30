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
app.config['SECRET_KEY'] = 'frenzy2018datafeedsearchengine'

def Wrap(feed, url, subid):
	new_link = ' '
	if feed == 'rakuten':
		if str(url).startswith(''):
			new_link = 'http://click.linksynergy.com/link?' + url + '&u1=' + subid
		if str(url).startswith('http'):
			new_link = url + '&u1=' + subid

	if feed == 'cj':
		if str(url).startswith('url'):
			new_link = 'http://www.dpbolvw.net/click-8810497-11192081?' + 'sid='+ subid + '&' + url 
		if str(url).startswith('http://'):
			new_link = url[0:url.find('?')]+'sid='+ subid + '&' + url[url.find('?'):]

	if feed == 'Pepperjam':
		if str(url).startswith('url'):
			new_link = '	http://www.pjtra.com/t/Qz9JSkJGP0NKRURGRT9JSkJG?' + 'sid='+ subid + '&' + url 
		if str(url).startswith('http://'):
			new_link = url[0: url.find('?')]+'sid='+ subid + '&' + url[url.find('?'):]

	if feed == 'ShareASale':
		new_link = url + 'afftrack=' + subid

	return new_link


def searchproducts(brand, category, subid):

	client = boto3.client('cloudsearchdomain',
	endpoint_url = 'http://search-test-frenzy-search-bjus6b5jv5bmi3cnrb3fqz2jc4.us-west-1.cloudsearch.amazonaws.com')
	###  change request content
	request = "(and brand:'brandname' category:'categoryname')"
	request = request.replace("brandname",brand).replace("categoryname",category)
	#####  change search paremeters
	response = client.search(query=request,
		    queryParser='structured',
		    returnFields="brand,category,product_name,brand,product_url,image_url,description,color,material,merch_id,merchant_name,price,size,source",
		    size=10000) #maxiamum size is 10,000

	for i in response['hits']['hit']:
		Wrap( i['fields']['source'][0] , i['fields']['product_url'][0], subid)

	result = response['hits']['hit']

	if response['hits']['hit']:
		return jsonify(result), 200

	return "product not found", 404


def process_query(json_query):
	json_dict = json.loads(json_query)
	number = len(json_dict['hits']['hit'])
	new_file = {"search_count":number, "hits":{}}
	count = 1
	for item in json_dict['hits']['hit']:
		brand = item['brand']
		category = item['category']
		subID = item['sub_ID']
		new_file["hits"]=searchproducts(brand,category,subID)
		count +=1
	with open('result.json','w') as f:
		json.dump(new_file,f)



@app.route('/', methods=['GET'])
def home():
    return "<h1>This is Frenzy Search: </h1><p>This site is a prototype API for search products according to key word.</p>"


@app.route('/upload', methods = ['POST'])
def api_upload():
    if request.headers['Content-Type'] == 'application/json':
        json_content = json.dumps(request.json)
        process_query(json_content)
        file = send_file('result.json', as_attachment=True)
        os.system('rm result.json')
        return file
    else:
        return "415 Unsupported Media Type ;(\n"



if __name__ == '__main__':
     app.run(port='5002')


