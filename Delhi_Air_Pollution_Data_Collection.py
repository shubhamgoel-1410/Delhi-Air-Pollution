#!/usr/bin/env python
# coding: utf-8

# In[65]:


# importing libraries
import pandas as pd
import schedule
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import time
import sys
import json


# In[45]:


# making list of all the url from which the data is to be collected
url_list = ['http://www.dpccairdata.com/dpccairdata/display/AallStationView5MinData.php?stName=UG9vdGhLaHVyZEJhd2FuYQ==',
           'http://www.dpccairdata.com/dpccairdata/display/AallStationView5MinData.php?stName=TmVocnVOYWdhcg==',
           'http://www.dpccairdata.com/dpccairdata/display/AallStationView5MinData.php?stName=SkxOU3RhZGl1bQ==',
           'http://www.dpccairdata.com/dpccairdata/display/AallStationView5MinData.php?stName=S2FybmlTaW5naFNob290aW5nUmFuZ2U=',
           'http://www.dpccairdata.com/dpccairdata/display/AallStationView5MinData.php?stName=TmF0aW9uYWxTdGFkaXVt',
           'http://www.dpccairdata.com/dpccairdata/display/AallStationView5MinData.php?stName=UGF0cGFyZ2Fuag==',
           'http://www.dpccairdata.com/dpccairdata/display/AallStationView5MinData.php?stName=Vml2ZWtWaWhhcg==',
           'http://www.dpccairdata.com/dpccairdata/display/AallStationView5MinData.php?stName=U29uaWFWaWhhcg==',
           'http://www.dpccairdata.com/dpccairdata/display/AallStationView5MinData.php?stName=TmFyZWxh',
           'http://www.dpccairdata.com/dpccairdata/display/AallStationView5MinData.php?stName=TmFqYWZnYXJo',
           'http://www.dpccairdata.com/dpccairdata/display/AallStationView5MinData.php?stName=Um9oaW5pU2VjdG9yMTY=',
           'http://www.dpccairdata.com/dpccairdata/display/AallStationView5MinData.php?stName=T2tobGFQaGFzZTI=',
           'http://www.dpccairdata.com/dpccairdata/display/AallStationView5MinData.php?stName=QXNob2tWaWhhcg==',
           'http://www.dpccairdata.com/dpccairdata/display/AallStationView5MinData.php?stName=V2F6aXJwdXI=',
           'http://www.dpccairdata.com/dpccairdata/display/AallStationView5MinData.php?stName=SmFoYW5naXJwdXJp',
           'http://www.dpccairdata.com/dpccairdata/display/AallStationView5MinData.php?stName=RHdhcmthU2VjdHJvOA==',
           'http://www.dpccairdata.com/dpccairdata/display/AallStationView5MinData.php?stName=QWxpcHVy',
           'http://www.dpccairdata.com/dpccairdata/display/AallStationView5MinData.php?stName=UHVzYQ==',
           'http://www.dpccairdata.com/dpccairdata/display/AallStationView5MinData.php?stName=U3JpQXVyYmluZG9NYXJn',
           'http://www.dpccairdata.com/dpccairdata/display/AallStationView5MinData.php?stName=TXVuZGth',
           'http://www.dpccairdata.com/dpccairdata/display/AallStationView5MinData.php?stName=QW5hbmRWaWhhcg==',
           'http://www.dpccairdata.com/dpccairdata/display/AallStationView5MinData.php?stName=TWFuZGlybWFyZw==',
           'http://www.dpccairdata.com/dpccairdata/display/AallStationView5MinData.php?stName=UHVuamFiaUJhZ2g=',
           'http://www.dpccairdata.com/dpccairdata/display/AallStationView5MinData.php?stName=UktQdXJhbQ==']


scope = ["https://spreadsheets.google.com/feeds",'https://www.googleapis.com/auth/spreadsheets',"https://www.googleapis.com/auth/drive.file","https://www.googleapis.com/auth/drive"]
# creds = ServiceAccountCredentials.from_json_keyfile_name("cred.json", scope)
print(str(sys.argv[1]))
creds = ServiceAccountCredentials.from_json(json.loads(json.dumps(str(sys.argv[1]))))
client = gspread.authorize(creds)
gas_sheet = client.open('Delhi_air_pollution').worksheet('gas_value')
meter_sheet = client.open('Delhi_air_pollution').worksheet('meter_value')


# reading all the tables in the web page 
df_list = pd.read_html('http://www.dpccairdata.com/dpccairdata/display/AallStationView5MinData.php?stName=UG9vdGhLaHVyZEJhd2FuYQ==')
# df_list is a list of all the tables present on the webpage. 





# making list of all the places from which different reading needs to be collected
places = list(map(str.strip,df_list[3][0][1].split('||')))[:-3]
print('Uncleaned List of Places ---> ',places)
print('')
# making correction in places list which are not read correctly   
correct_place = places[4].split()
places[4] = " ".join(correct_place[:5])
places.insert(5,correct_place[5])

correct_place = places[15].split()
places[15] = " ".join(correct_place[:3])
places.insert(16,correct_place[3])
print('Cleaned List of Places ---> ',places)



# making a dict of places with their url
url_places = dict(zip(places,url_list))
url_places


# ### Problem:
# Like the data coming from the web was having very duplicate values like date and time 
# so used pivot to get the desired format for a table


def extract_data(x,value):
    
    x = x.iloc[:,:4]
    
    # Making new Columns from Date having Day and date seprated.
    x[['Day','Date']] = x.Date.str.split(',',1,expand=True)

    # Merging the Date and Time Columns to make a new column 
    x['TimeStamp'] = x['Date'].str.cat(x["Time  (IST)"], sep =" ")

    #dropping the old Date and time columns
    x = x.drop(columns = ['Date','Time  (IST)'])
 
    # using Pivot to get rid of the duplicate entry for day and timestamp columns
    x = x.pivot_table(index=['Day','TimeStamp'], columns='Parameters',values = value ,aggfunc='sum')
    x = x.reset_index()
    return (x.iloc[0],x.columns)



gas_data, gas_table_columns = extract_data(df_list[5],'Gas Concentrations')  # GAS CONCENTRATIONS DATA FOR A PLACE
meter_data, meter_table_columns = extract_data(df_list[8],'Concentration')  #PARTICULATE CONCENTRATION & METEOROLOGICAL CONDITION

gas_table_columns = gas_table_columns.insert(0,'Place')
meter_table_columns = meter_table_columns.insert(0,'Place')

gas_sheet.append_row(list(gas_table_columns))
meter_sheet.append_row(list(meter_table_columns))


def collect_data():
    index_of_gas_data = 5
    index_of_meter_data = 8

    for place,url in url_places.items():
        df_list = pd.read_html(url)
        
        gas_data, gas_table_columns = extract_data(df_list[index_of_gas_data],'Gas Concentrations')
        meter_data, meter_table_columns = extract_data(df_list[index_of_meter_data],'Concentration')
        
        gas_data = pd.Series(place).append(gas_data)
        meter_data = pd.Series(place).append(meter_data)
        
        gas_sheet.append_row(list(gas_data))
        meter_sheet.append_row(list(meter_data))
    
while(True):
    collect_data()
    time.sleep(300)




