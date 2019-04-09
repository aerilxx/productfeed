from celery import Celery
import boto3
from botocore.client import Config


app = Celery('upload_tasks', broker='pyamqp://localhost')

@app.task
# take json str (not file) as input, turn it into bytes and upload it to cloudsearch
def upload(chuck):
	# Both timeouts default to 60, you can customize them, seperately
	config = Config(connect_timeout=70)
	products_bytes = chuck.encode('utf-8')
	try:
		client = boto3.client('cloudsearchdomain',endpoint_url = 'http://doc-test-frenzy-search-bjus6b5jv5bmi3cnrb3fqz2jc4.us-west-1.cloudsearch.amazonaws.com',config=config)
		response = client.upload_documents(documents= products_bytes,contentType='application/json')
	except botocore.errorfactory.DocumentServiceException:
		print ('ignore document error..')
		
	return str(response['status'])
    
