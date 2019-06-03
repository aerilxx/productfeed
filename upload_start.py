import json
from upload_task import upload
import os


# chucnk json file into 5 mb and use parallel uploading
def divide(bigfile, batch_size):

    for i in range(0, len(bigfile), batch_size):  
        yield bigfile[i:i + batch_size] 


def chunck_file(json_file):
    statinfo = os.stat(json_file)
    if statinfo.st_size > 5000000:
        with open(json_file) as j:
            try:
                data = json.load(j)
                for i in divide(data, 4500):
                    i_data = json.dumps(i)
                    upload.delay(i_data)
                    data.pop(4500)
                open(json_file,'w').write(json.dumps(data,indent=4))
            except json.decoder.JSONDecodeError:
                print ('Decoding JSON has failed')
        
        os.remove(json_file)
    # for file smaller than 5MB, upload as whole        
    else:
        with open(json_file) as j:
            try:
                data = json.load(j)
                i_data = json.dumps(data)
                upload.delay(i_data)
                os.remove(json_file)
            except json.decoder.JSONDecodeError:
                print ('Decoding JSON has failed')
            

if __name__ == '__main__':
    for f in os.listdir('.'):
        if f.endswith('.json'):
            chunck_file(f)
            #os.remove(f)


    for f in os.listdir('.'):
        if f.endswith('xml') or f.endswith('gz'):
            os.remove(f)
            
    print ('upload down...')

