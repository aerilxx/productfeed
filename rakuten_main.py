import gzip
from ftplib import FTP
from rakuten_celery import download, upload
import os

#connect to sftp and get file list
def connect():

	def create_file_list(files):

		'''
			merch_id = re.search(r'([0-9]*)_',x).group(1)
            # If the merch_id don't exist in cloudsearch, put the full catelog file's name in the file_list (one that has "mp.xml")
            client = boto3.client('cloudsearchdomain', endpoint_url = 'http://search-frenzysearch-g2cbqftgpmzftr7am5fvng5sz4.us-west-1.cloudsearch.amazonaws.com')
            response = client.search(
                query='merch_id:'+ str(merch_id),
                queryParser='lucene',
                size=10)
            if response['hits']['found'] == 0:
            	file_list.append(x)
        '''  

		file_list = []
		for x in files:
			if 'mp.xml' in x:
				file_list.append(x)
			if 'delta.xml' in x:
				file_list.append(x)

		return file_list

	ftp = FTP('aftp.linksynergy.com')
	ftp.login('frenzylabs','V9P26CQ')
	ftp_list = ftp.nlst()

	file_list = create_file_list(ftp_list)

	return file_list
	

if __name__ == '__main__':

	for i in connect():
		json_list = download.delay(i)

	filelist = [ f for f in os.listdir() if f.endswith('.xml')]

	for f in filelist:
		os.remove(f)  

	print ('rakuten downlowd over....')










