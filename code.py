#[cloninig Libraries]
import os
import git
import subprocess
import requests
import json

#[Mysql Libraries]
import mysql.connector
import sqlalchemy
from sqlalchemy import create_engine

#[Visualization Libraries]
import pandas as pd

#------------------------------------------------------------cloning the github repository ------------------------------------------------------------------------------------------

# Function to clone the repository to the specified local directory
def clone_repository(repo_url, clone_dir):
    try:
        subprocess.run(['git', 'clone', repo_url, clone_dir], check=True)
        return True  # Return True if the repository is cloned successfully
    except subprocess.CalledProcessError as e:
        return False, f"Error while cloning the repository: {e}"  # Return False and the error message
    except Exception as e:
        return False, f"An unexpected error occurred: {e}"  # Return False and the error message

# Specify the GitHub repository URL
response = requests.get('https://api.github.com/repos/PhonePe/pulse')
json_data = response.json()

# Extract the clone URL from the JSON data.
phonepe_pulse_repo_url = json_data['clone_url']
clone_dir = 'D:/Data Science/project_data'

# Check if the target directory already exists and is not empty
if not os.path.exists(clone_dir) or not os.listdir(clone_dir):
    success, message = clone_repository(phonepe_pulse_repo_url, clone_dir)
    if success:
        print("Repository cloned successfully!")
    else:
        print(message)
else:
    print("Target directory already exists and is not empty. Skipping the cloning process.")


#-----------------------------------------------------------------Aggregated state data-----------------------------------------------------------------------

#Extracting the data from the specified path folder
path = 'D:/Data Science/project_data/data/aggregated/transaction/country/india/state/'
Agg_state_list = os.listdir(path)

def json_dataframe_agg_state_list(path, Agg_state_list):
    column1 = {'State': [], 'Year': [], 'Quarter': [], 'Transaction_type': [], 'Transaction_count': [], 'Transaction_amount': []}

    for i in Agg_state_list:
        p_i = os.path.join(path, i)
        Agg_yr_list = os.listdir(p_i)

        for j in Agg_yr_list:
            p_j = os.path.join(p_i, j)
            Agg_qtr_list = os.listdir(p_j)

            for k in Agg_qtr_list:
                p_k = os.path.join(p_j, k)
                with open(p_k, 'r') as Data:
                    D = json.load(Data)

                for z in D['data']['transactionData']:
                    column1['State'].append(i)
                    column1['Year'].append(j)
                    name = z['name']
                    count = z['paymentInstruments'][0]['count']
                    amount = z['paymentInstruments'][0]['amount']
                    column1['Transaction_type'].append(name)
                    column1['Transaction_count'].append(count)
                    column1['Transaction_amount'].append(amount)
                    column1['Quarter'].append(int(k.strip('.json')))

    df1 = pd.DataFrame(column1)
    return df1
Agg_list = json_dataframe_agg_state_list(path,Agg_state_list)
#print(Agg_list)

#-----------------------------------------------------------------Aggregated user data-----------------------------------------------------------------------


#Extracting the data from the specified path folder
path_1 = 'D:/Data Science/project_data/data/aggregated/user/country/india/state/'
Agg_user_state_list = os.listdir(path_1)
def json_dataframe_agg_user_list(path_1, Agg_user_state_list):
    column1 = {'State': [], 'Year': [], 'Quarter': [], 'Brands': [], 'User_Count': [], 'User_Percentage': []}

    for i in Agg_user_state_list:
        p_i = os.path.join(path_1, i)
        Agg_yr_list = os.listdir(p_i)

        for j in Agg_yr_list:
            p_j = os.path.join(p_i, j)
            Agg_qtr_list = os.listdir(p_j)

            for k in Agg_qtr_list:
                p_k = os.path.join(p_j, k)
                with open(p_k, 'r') as Data:
                    D = json.load(Data)

                try:

                    for z in D['data']['usersByDevice']:
                        column1['State'].append(i)
                        column1['Year'].append(j)
                        column1['Quarter'].append(int(k.strip('.json')))
                        brand_name = z['brand']
                        count = z['count']
                        total_percentage = z['percentage']
                        column1['Brands'].append(brand_name)
                        column1['User_Count'].append(count)
                        column1['User_Percentage'].append(total_percentage*100)
                        
                except:
                    pass

    df2 = pd.DataFrame(column1)
    return df2
Agg_user_list = json_dataframe_agg_user_list(path_1,Agg_user_state_list)
#print(Agg_user_list)


#-----------------------------------------------------------------Map transaction data-----------------------------------------------------------------------

#Extracting the data from the specified path folder
path_2 = 'D:/Data Science/project_data/data/map/transaction/hover/country/india/state/'
Map_state_list = os.listdir(path_2)

def json_dataframe_map_state_list(path_2, Map_state_list):
    column1 = {'State': [], 'Year': [], 'Quarter': [], 'District': [], 'Transaction_count': [], 'Transaction_amount': []}

    for i in Map_state_list:
        p_i = os.path.join(path_2, i)
        Agg_yr_list = os.listdir(p_i)

        for j in Agg_yr_list:
            p_j = os.path.join(p_i, j)
            Agg_qtr_list = os.listdir(p_j)

            for k in Agg_qtr_list:
                p_k = os.path.join(p_j, k)
                with open(p_k, 'r') as Data:
                    D = json.load(Data)

                for z in D['data']['hoverDataList']:
                    column1['State'].append(i)
                    column1['Year'].append(j)
                    District = z['name']
                    count = z['metric'][0]['count']
                    amount = z['metric'][0]['amount']
                    column1['District'].append(District)
                    column1['Transaction_count'].append(count)
                    column1['Transaction_amount'].append(amount)
                    column1['Quarter'].append(int(k.strip('.json')))

    df3 = pd.DataFrame(column1)
    return df3
Map_list = json_dataframe_map_state_list(path_2,Map_state_list)
#print(Map_list)



#-----------------------------------------------------------------Map user data-----------------------------------------------------------------------


#Extracting the data from the specified path folder
path_3 = 'D:/Data Science/project_data/data/map/user/hover/country/india/state/'
Map_user_state_list = os.listdir(path_3)
def json_dataframe_map_user_list(path_3, Map_user_state_list):
    column1 = {'State': [], 'Year': [], 'Quarter': [], 'District': [], 'Registered_User': []}

    for i in Map_user_state_list:
        p_i = os.path.join(path_3, i)
        Agg_yr_list = os.listdir(p_i)

        for j in Agg_yr_list:
            p_j = os.path.join(p_i, j)
            Agg_qtr_list = os.listdir(p_j)

            for k in Agg_qtr_list:
                p_k = os.path.join(p_j, k)
                with open(p_k, 'r') as Data:
                    D = json.load(Data)

                for z in D['data']['hoverData'].items():
                    column1['State'].append(i)
                    column1['Year'].append(j)
                    column1['Quarter'].append(int(k.strip('.json')))
                    district = z[0]
                    registered_user = z[1]['registeredUsers']
                    column1['District'].append(district)
                    column1['Registered_User'].append(registered_user)
                        
    df4 = pd.DataFrame(column1)
    return df4
Map_user_list = json_dataframe_map_user_list(path_3,Map_user_state_list)
#print(Map_user_list)



#-----------------------------------------------------------------Top transaction data-----------------------------------------------------------------------


#Extracting the data from the specified path folder
path_4 = 'D:/Data Science/project_data/data/top/transaction/country/india/state/'
Top_trans_state_list = os.listdir(path_4)
def json_dataframe_top_trans_state_list(path_4, Top_trans_state_list):
    column1 = {'State': [], 'Year': [], 'Quarter': [], 'District_Pincode': [], 'Transaction_count': [], 'Transaction_amount': []}

    for i in Top_trans_state_list:
        p_i = os.path.join(path_4, i)
        Agg_yr_list = os.listdir(p_i)

        for j in Agg_yr_list:
            p_j = os.path.join(p_i, j)
            Agg_qtr_list = os.listdir(p_j)

            for k in Agg_qtr_list:
                p_k = os.path.join(p_j, k)
                with open(p_k, 'r') as Data:
                    D = json.load(Data)

                for z in D['data']['pincodes']:
                    column1['State'].append(i)
                    column1['Year'].append(j)
                    column1['Quarter'].append(int(k.strip('.json')))
                    name = z['entityName']
                    count = z['metric']['count']
                    amount = z['metric']['amount']
                    column1['District_Pincode'].append(name)
                    column1['Transaction_count'].append(count)
                    column1['Transaction_amount'].append(amount)
                        
    df5 = pd.DataFrame(column1)
    return df5
Top_trans_list = json_dataframe_top_trans_state_list(path_4,Top_trans_state_list)
#print(Top_trans_list)


#-----------------------------------------------------------------Top user data-----------------------------------------------------------------------


#Extracting the data from the specified path folder
path_5 = 'D:/Data Science/project_data/data/top/user/country/india/state/'
Top_user_state_list = os.listdir(path_5)
def json_dataframe_top_user_list(path_5, Top_user_state_list):
    column1 = {'State': [], 'Year': [], 'Quarter': [], 'District_Pincode': [], 'Registered_User': []}

    for i in Top_user_state_list:
        p_i = os.path.join(path_5, i)
        Agg_yr_list = os.listdir(p_i)

        for j in Agg_yr_list:
            p_j = os.path.join(p_i, j)
            Agg_qtr_list = os.listdir(p_j)

            for k in Agg_qtr_list:
                p_k = os.path.join(p_j, k)
                with open(p_k, 'r') as Data:
                    D = json.load(Data)

                for z in D['data']['pincodes']:
                    column1['State'].append(i)
                    column1['Year'].append(j)
                    column1['Quarter'].append(int(k.strip('.json')))
                    name = z['name']
                    registered_user = z['registeredUsers']
                    column1['District_Pincode'].append(name)
                    column1['Registered_User'].append(registered_user)
                        
    df6 = pd.DataFrame(column1)
    return df6
Top_user_list = json_dataframe_top_user_list(path_5,Top_user_state_list)
#print(Top_user_list)

#============================================Mysql connector================================================================================

#create the connection and create the database
mydb = mysql.connector.connect(
                host = 'localhost',
                user = 'root',
                port = '3306',
                password='365pass',
                auth_plugin = "mysql_native_password")
mycursor = mydb.cursor()
mycursor.execute('CREATE DATABASE IF NOT EXISTS phonepe_pulse')

#close the connection and database
mycursor.close()
mydb.close()

db_url = "mysql+mysqlconnector://root:365pass@localhost:3306/phonepe_pulse"
engine = create_engine(db_url,echo=True)

#creating dataframes and sending it to sql

#1.sending dataframes to sql of Agg_list
Agg_list.to_sql('aggregrated_transaction_details',con=engine,if_exists='replace',index=False,
                dtype={'State':sqlalchemy.types.VARCHAR(length=50),
                       'Year':sqlalchemy.types.Integer,
                       'Quarter':sqlalchemy.types.INTEGER,
                       'Transaction_type':sqlalchemy.types.VARCHAR(length=50),
                       'Transaction_count':sqlalchemy.types.Integer,
                       'Transaction_amount':sqlalchemy.types.FLOAT(precision=5,asdecimal=True)})

#2.sending dataframes to sql of Agg_user_list
Agg_user_list.to_sql('aggregrated_user_details',con=engine,if_exists='replace',index=False,
                     dtype={'State':sqlalchemy.types.VARCHAR(length=50),
                            'Year':sqlalchemy.types.Integer,
                            'Quarter':sqlalchemy.types.Integer,
                            'Brands':sqlalchemy.types.VARCHAR(length=50),
                            'User_Count':sqlalchemy.types.Integer,
                            'User_Percentage':sqlalchemy.types.FLOAT(precision=5,asdecimal=True)})

#3.sending dataframes to sql of Map_list
Map_list.to_sql('map_transaction_details',con=engine,if_exists='replace',index=False,
                dtype={'State':sqlalchemy.types.VARCHAR(length=50),
                       'Year':sqlalchemy.types.Integer,
                       'Quarter':sqlalchemy.types.INTEGER,
                       'District':sqlalchemy.types.VARCHAR(length=50),
                       'Transaction_count':sqlalchemy.types.Integer,
                       'Transaction_amount':sqlalchemy.types.FLOAT(precision=5,asdecimal=True)})

#4.sending dataframes to sql of Map_user_list
Map_user_list.to_sql('map_user_details',con=engine,if_exists='replace',index=False,
                     dtype={'State':sqlalchemy.types.VARCHAR(length=50),
                            'Year':sqlalchemy.types.Integer,
                            'Quarter':sqlalchemy.types.Integer,
                            'District':sqlalchemy.types.VARCHAR(length=50),
                            'Registered_User':sqlalchemy.types.Integer})

#5.sending dataframes to sql of Top_trans_list
Top_trans_list.to_sql('top_transaction_detials',con=engine,if_exists='replace',index=False,
                      dtype={'State':sqlalchemy.types.VARCHAR(length=50),
                             'Year':sqlalchemy.types.Integer,
                             'Quarter':sqlalchemy.types.INTEGER,
                             'District_Pincode':sqlalchemy.types.Integer,
                             'Transaction_count':sqlalchemy.types.Integer,
                             'Transaction_amount':sqlalchemy.types.FLOAT(precision=5,asdecimal=True)})


#6.sending dataframes to sql of Top_user_list
Top_user_list.to_sql('top_user_details',con=engine,if_exists='replace',index=False,
                     dtype={'State':sqlalchemy.types.VARCHAR(length=50),
                            'Year':sqlalchemy.types.Integer,
                            'Quarter':sqlalchemy.types.Integer,
                            'District_Pincode':sqlalchemy.types.Integer,
                            'Registered_User':sqlalchemy.types.Integer})





