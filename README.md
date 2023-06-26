# phone.pe_pulse

**phone pe pulse data visualisation**
**The Necessary libraries for deploying this project are:**
import mysql.connector,
import os,
import pandas as pd,
import json,
import sqlalchemy,
from sqlalchemy import create_engine,
import streamlit as st,
import requests,
import plotly.express as px.

**Data Extraction** :
Clone the Github using scripting to fetch the data from the Phonepe pulse Github repository and store it in a suitable format such as JSON. Use the below syntax to clone the phonepe github repository into your local drive.
git clone https://github.com/PhonePe/pulse.git

**Data transformation:**

In this step the JSON files that are available in the folders are converted into the readeable and understandable DataFrame format by using the for loop and iterating file by file and then finally the DataFrame is created. In order to perform this step I've used os, json and pandas packages. And finally converted the dataframe into CSV file and storing in the local drive.

path1 = "Path of the JSON files"

agg_trans_list = os.listdir(path1)

# The user can give any column name and use any variable inside the for loop according to his/her convenience.
column = {'State': [], 'Year': [], 'Quarter': [], 'Transaction_type': [], 'Transaction_count': [],'Transaction_amount': []}
Looping through each and every folder and opening the json files appending only the required key and values and creating the dataframe.

Note: I have shown creation of one dataframe here.Six dataframes have been created just like the one shown here.
  for state in agg_trans_list:
    cur_state = path1 + state + "/"
    agg_year_list = os.listdir(cur_state)

    for j in agg_year_list:
        cur_year = cur_state + j + "/"
        agg_file_list = os.listdir(cur_year)

        for k in agg_file_list:
            cur_file = cur_year + k
            data = open(cur_file, 'r')
            A = json.load(data)

            for i in A['data']['transactionData']:
                name = i['name']
                count = i['paymentInstruments'][0]['count']
                amount = i['paymentInstruments'][0]['amount']
                column['Transaction_type'].append(name)
                column['Transaction_count'].append(count)
                column['Transaction_amount'].append(amount)
                column['State'].append(state)
                column['Year'].append(year)
                column['Quarter'].append(int(k.strip('.json')))
   df_aggregated_transaction=pd.DataFrame(column)
            
**Data Insertion:**
After creating DataFrames of given pulse data, insert it into SQL database and create diffrent tables for diffrent aggregated datas.
six tables have been created, namely:
1. aggregated_transaction
2. aggregated_user
3. map_transaction
4. map_usesr
5. top_transaction
6. top_user

**Dashboard creation:**
To create colourful and insightful dashboard I've used Plotly libraries in Python to create an interactive and visually appealing dashboard. Plotly's built-in functions like Bar and Geo map are used to display the data on a charts and map and Streamlit is used to create a user-friendly interface with multiple dropdown options for users to select different facts and figures to display.





