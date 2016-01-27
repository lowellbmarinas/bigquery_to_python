# -*- coding: utf-8 -*-
"""
Created on Thu Jan 21 19:52:10 2016
@author: lowellmarinas
"""
#CHECKS CURRENT WORKING DIRECTORY AND CHANGE IF NECESSARY IN ORDER TO ADD PKCS12 KEY
import os
os.getcwd()
#PKCS12 Key as well as intermediary flat files will be stored here
os.chdir('/Users/lowellmarinas/Google Drive/Zuellig Pharma/Solutions/Data Analytics/GCP/')
from bigquery import get_client
# BigQuery project id as listed in the Google Developers Console.
project_id = 'zp-analytics'
# Service account email address as listed in the Google Developers Console.
service_account = 'tradeinfrared@zp-analytics.iam.gserviceaccount.com'
# PKCS12 or PEM key provided by Google.
key = 'zp-analytics-851312439634.p12'
client = get_client(project_id, service_account=service_account,
                    private_key_file=key, readonly=False)
#IMPORTS DATA FROM THE GCP
from pandas.io import gbq
import pandas as pd
import numpy as np
sProjectID = 'zp-analytics'
#Imports the Item Code
sQuery = 'SELECT Material_Number FROM SELL_IN_DATA.zpsg'
Material_Number = gbq.read_gbq(sQuery, sProjectID)
sg_ts_data = Material_Number
#Imports the Purchaser's Code
sQuery = 'SELECT Sold_to_Party FROM SELL_IN_DATA.zpsg'
Sold_to_Party = gbq.read_gbq(sQuery, sProjectID)
sg_ts_data['Sold_to_Party'] = Sold_to_Party
#Imports the Receiver's Code
sQuery = 'SELECT Ship_To_Party FROM SELL_IN_DATA.zpsg'
Ship_To_Party = gbq.read_gbq(sQuery, sProjectID)
sg_ts_data['Ship_To_Party'] = Ship_To_Party
#Imports the Date of order Code - should be Billing Date
sQuery = 'SELECT Order_Request_Date FROM SELL_IN_DATA.zpsg'
Order_Request_Date = gbq.read_gbq(sQuery, sProjectID)
sg_ts_data['Order_Request_Date'] = Order_Request_Date
#Imports the number of items that were consumed
sQuery = 'SELECT Billing_Quantity FROM SELL_IN_DATA.zpsg'
Billing_Quantity = gbq.read_gbq(sQuery, sProjectID)
sg_ts_data['Billing_Quantity'] = Billing_Quantity
#CONVERT INCOMING GBQ DATAFRAME TO REGULAR PANDAS DATAFRAME
sg_ts_data = pd.DataFrame(sg_ts_data)
#having trouble converting the billing date column of the pandas data frame to string
#BigQuery feature in Python is still experimental - thus, conversion may not be fully stabiized
sg_ts_data.to_csv('sg_ts_data.csv')
sg_ts_data = pd.read_csv('sg_ts_data.csv')
#Breaks billing date into just year month components
#Allows aggregation into these buckets using pivot tables
pd.options.mode.chained_assignment = None  # default='warn'
test = (sg_ts_data['Order_Request_Date'])
for i in range(0,len(sg_ts_data['Order_Request_Date'])):
    test[i] = sg_ts_data['Order_Request_Date'][i].astype('|S6')
    print (i)
sg_ts_data['month_year']=test
#ZEROS FILE FOR TIME SERIES ANALYSIS using Pivot Tables
TS_zpsg = pd.pivot_table(sg_ts_data, 
                         index=['Material_Number', #Index by material number
                         'Sold_to_Party',           #With Buyer Code
                         'Ship_To_Party'],          #With Receiveer
                        values=['Billing_Quantity'],#Summate by quantity consumed
                        columns='month_year',       #In year_month bucket
                        aggfunc=[np.sum],fill_value=0) #During times of no consumption, 0 is entered
#Converts data from wide to long format along the year_month bucket
stacked_sg = TS_zpsg.stack()
stacked_sg.to_csv('TimeSeriesZPSG.csv')
#Saves data as a flat file into disk in order to remove the miscellaneous confusing headers
stacked_sg = pd.read_csv('TimeSeriesZPSG.csv',
                         dtype={'month_year':'string',
                             'Sold_to_Party': 'object',
                             'Ship_To_Party': 'object',
                             'Material_Number': 'object'},
                             header=2)
stacked_sg.rename(columns={'Unnamed: 4': 'Billing_Quantity'}, inplace=True)
#STRIPS MISCELLANEOUS INFORMATION
stacked_sg['Year']=stacked_sg['month_year'].str[0:4]
stacked_sg['Month']=stacked_sg['month_year'].str[4:6]
stacked_sg['Date']=stacked_sg['Year']+"-"+stacked_sg['Month']+"-01"
stacked_sg['Date']=pd.to_datetime(stacked_sg['Date'])
stacked_sg.drop('month_year',axis=1,inplace=True)
stacked_sg.drop('Year',axis=1,inplace=True)
stacked_sg.drop('Month',axis=1,inplace=True)
#STREAM DATA INTO BIGQUERY
for i in range(1,100):
    stream=stacked_sg.iloc[[i]]
    gbq.to_gbq(stream,'TIME_SERIES.zpsg','zp-analytics',verbose=True,if_exists='append')
