# download file from ftp and preprocessing
import gzip
import paramiko
from cj_celery import download
import os

#connect to sftp and get file list

def connect():
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        ssh.connect(hostname="datatransfer.cj.com",username="5102504",password="v7~MXEym")
    except paramiko.SSHException:
        print("Connection Failed")
        quit()

    sftp = ssh.open_sftp()
    sftp.chdir("/outgoing/productcatalog/217585/")
    file_list = sftp.listdir()

    return file_list


if __name__ == '__main__':
    for i in connect():
        json_list = download.delay(i)

    filelist = [ f for f in os.listdir() if f.endswith('.xml')]

    for f in filelist:
        os.remove(f)  

    print ('cj download over....')




