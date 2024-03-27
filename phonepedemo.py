import pandas as pd
import json
import os
import psycopg2

# already the phonepe github clone is added to the file 

# AGGREGATED_TRANSACTION 

path="D:/docs/PHONEPE/pulse/data/aggregated/transaction/country/india/state/"
Agg_state_list = os.listdir(path)
Agg_state_list


col = {'State':[],'Year':[],'Quater':[],'Transaction_type':[],'Transaction_count':[],'Transaction_amount':[]}


for i in Agg_state_list:
    p_i=path+i+"/"
    Agg_yr=os.listdir(p_i)
    for j in Agg_yr:
        p_j=p_i+j+"/"
        Agg_yr_list=os.listdir(p_j)
        for k in Agg_yr_list:
            p_k=p_j+k
            Data=open(p_k,'r')
            D=json.load(Data)
            for z in D['data']['transactionData']:
              Name=z['name']
              count=z['paymentInstruments'][0]['count']
              amount=z['paymentInstruments'][0]['amount']
              col['Transaction_type'].append(Name)
              col['Transaction_count'].append(count)
              col['Transaction_amount'].append(amount)
              col['State'].append(i)
              col['Year'].append(j)
              col['Quater'].append(int(k.strip('.json')))
#Succesfully created a dataframe
Agg_Trans=pd.DataFrame(col)

# AGGREGATED_USERS 

path="D:/docs/PHONEPE/pulse/data/aggregated/user/country/india/state/"
user_state_list = os.listdir(path)
user_state_list


col1 = {'State': [], 'Year': [], 'Quarter': [], 'Transaction_brand': [], 'Transaction_count': [], 'Transaction_percentage': []}

for i in user_state_list:
    p_i = path + i + "/"
    user_yr = os.listdir(p_i)
    for j in user_yr:
        p_j = p_i + j + "/"
        user_qtr_list = os.listdir(p_j)
        for k in user_qtr_list:
            p_k = p_j + k
            with open(p_k, 'r') as Data:
                try:
                    D = json.load(Data)
                    for z in D.get('data', {}).get('usersByDevice', []):
                        brand = z.get('brand')
                        count = z.get('count')
                        percentage = z.get('percentage')
                        if brand is not None and count is not None and percentage is not None:
                            col1['State'].append(i)
                            col1['Year'].append(j)
                            col1['Quarter'].append(int(k.strip('.json')))
                            col1['Transaction_brand'].append(brand)
                            col1['Transaction_count'].append(count)
                            col1['Transaction_percentage'].append(percentage)
                except Exception as e:
                    print(f"Error processing file {p_k}: {e}")

Agg_user = pd.DataFrame(col1)


# MAP_TRANSACTION 

path="D:/docs/PHONEPE/pulse/data/map/transaction/hover/country/india/state/"
map_state_list = os.listdir(path)
map_state_list

col2 = {'State': [], 'Year': [], 'Quarter': [], 'Transaction_type': [], 'Transaction_district': [], 'Transaction_count': [], 'Transaction_amount': []}

for i in map_state_list:
    p_i=path+i+"/"
    map_yr=os.listdir(p_i)
    for j in map_yr:
        p_j=p_i+j+"/"
        map_yr_list=os.listdir(p_j)
        for k in map_yr_list:
            p_k=p_j+k
            Data=open(p_k,'r')
            D=json.load(Data)
            for z in D['data']['hoverDataList']:
              name = z['name']
              ttype = z['metric'][0]['type']
              count = z['metric'][0]['count']
              amount = z['metric'][0]['amount']
              col2['Transaction_district'].append(name)
              col2['Transaction_type'].append(ttype)
              col2['Transaction_count'].append(count)
              col2['Transaction_amount'].append(amount)
              col2['State'].append(i)
              col2['Year'].append(j)
              col2['Quarter'].append(int(k.strip('.json')))

map_Trans=pd.DataFrame(col2)


#MAP_USERS 

path="D:/docs/PHONEPE/pulse/data/map/user/hover/country/india/state/"
mapuser_state_list = os.listdir(path)
mapuser_state_list

col3 = {'State': [], 'Year': [], 'Quater': [], 'District': [],'Registered_user': [], 'App_opening': []}

for i in mapuser_state_list:
    p_i = path+i+"/"
    year = os.listdir(p_i)
    for j in year:
        p_j = p_i+j+"/"
        file = os.listdir(p_j)
        for k in file:
            p_k = p_j+k
            Data = open(p_k, 'r')
            D = json.load(Data)
            try:
                for z in D['data']["hoverData"]:
                    district = z
                    registered_user =  D['data']["hoverData"][z]["registeredUsers"]
                    app_opening = D['data']["hoverData"][z]["appOpens"]
                    col3['District'].append(district)
                    col3['Registered_user'].append(registered_user)
                    col3['App_opening'].append(app_opening)
                    col3['State'].append(i)
                    col3['Year'].append(j)
                    col3['Quater'].append(int(k.strip('.json')))

            except:
              pass

map_users = pd.DataFrame(col3)


# TOP_TRANSACTION 

path="D:/docs/PHONEPE/pulse/data/top/transaction/country/india/state/"
top_trans_state_list = os.listdir(path)
top_trans_state_list

col4 = {'State': [], 'Year': [], 'Quarter': [], 'Transaction_type': [], 'Transaction_district': [], 'Transaction_count': [], 'Transaction_amount': []}


for i in top_trans_state_list:
    p_i=path+i+"/"
    top_trans_yr=os.listdir(p_i)
    for j in top_trans_yr:
        p_j=p_i+j+"/"
        top_trans_yr_list=os.listdir(p_j)
        for k in top_trans_yr_list:
            p_k=p_j+k
            Data=open(p_k,'r')
            D=json.load(Data)
            for z in D['data']['districts']:
              name = z['entityName']
              mtype = z['metric']['type']
              count = z['metric']['count']
              amount = z['metric']['amount']
              col4['Transaction_district'].append(name)
              col4['Transaction_type'].append(mtype)
              col4['Transaction_count'].append(count)
              col4['Transaction_amount'].append(amount)
              col4['State'].append(i)
              col4['Year'].append(j)
              col4['Quarter'].append(int(k.strip('.json')))


top_trans = pd.DataFrame(col4)


# TOP_USERS 

path="D:/docs/PHONEPE/pulse/data/top/user/country/india/state/"
top_users_state_list = os.listdir(path)
top_users_state_list

col5  = {'State': [], 'Year': [], 'Quarter': [], 'registeredusers': [], 'Transaction_district': []}

for i in top_users_state_list:
    p_i=path+i+"/"
    top_users_yr=os.listdir(p_i)
    for j in top_users_yr:
        p_j=p_i+j+"/"
        top_users_yr_list=os.listdir(p_j)
        for k in top_users_yr_list:
            p_k=p_j+k
            Data=open(p_k,'r')
            D=json.load(Data)
            for z in D['data']['districts']:
              name = z['name']
              registeredusers = z['registeredUsers']
              col5['Transaction_district'].append(name)
              col5['registeredusers'].append(registeredusers)
              col5['State'].append(i)
              col5['Year'].append(j)
              col5['Quarter'].append(int(k.strip('.json')))


top_users = pd.DataFrame(col5)



