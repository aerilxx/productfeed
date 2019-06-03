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
		client = boto3.client('cloudsearchdomain',endpoint_url = 'cloudsearc_document_url',config=config)
		response = client.upload_documents(documents= products_bytes,contentType='application/json')
		print(str(response['status']))
	except botocore.errorfactory.DocumentServiceException:
		print ('ignore document error..')
		
	return "done"
    
