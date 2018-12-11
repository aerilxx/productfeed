#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Oct 31 17:04:11 2018

@author: aeril
"""

import mysql.connector 
from mysql.connector import errorcode 

# dynamic connect to myphpadmin to grab userID 


def getUserId():
    query=(" SELECT UserID from Users order by UserID")
    SID_list=[]
    try:
        cnx = mysql.connector.connect(user='frenzylive', password='bhxJ6V1B',
                              host='frenzystaging.cimjunfbcdxs.us-west-1.rds.amazonaws.com',
                              database='ebdb')
     
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
        else:
            print(err)
            
    cursor=cnx.cursor()
    cursor.execute(query)
    for userID in cursor:
        SID_list.append(userID[0])

    cursor.close()    
    cnx.close()
    
    return SID_list
    print(SID_list)    
    
if __name__=="__main__":
    getUserId()
