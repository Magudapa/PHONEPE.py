import pandas as pd
import json
import os
import psycopg2



#Aggregated Transaction

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
Agg_Tran=pd.DataFrame(col)
              
            
Agg_Tran['State']=Agg_Tran['State'].str.replace('andaman-&-nicobar-islands','andaman & nicobar')
Agg_Tran['State']=Agg_Tran['State'].str.replace('jammu-&-kashmir', 'jammu & kashmir')
Agg_Tran['State']=Agg_Tran['State'].str.replace('dadra-&-nagar-haveli-&-daman-&-diu','dadra and nagar haveli and daman and diu')
Agg_Tran['State']=Agg_Tran['State'].str.replace("-"," ")
Agg_Tran['State']=Agg_Tran['State'].str.title()


#Aggregated User

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

Agg_user['State']=Agg_user['State'].str.replace('andaman-&-nicobar-islands','andaman & nicobar')
Agg_user['State']=Agg_user['State'].str.replace('jammu-&-kashmir', 'jammu & kashmir')
Agg_user['State']=Agg_user['State'].str.replace('dadra-&-nagar-haveli-&-daman-&-diu','dadra and nagar haveli and daman and diu')
Agg_user['State']=Agg_user['State'].str.replace("-"," ")
Agg_user['State']=Agg_user['State'].str.title()

#map transaction

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

map_trans=pd.DataFrame(col2)

map_trans['State']=map_trans['State'].str.replace('andaman-&-nicobar-islands','andaman & nicobar')
map_trans['State']=map_trans['State'].str.replace('jammu-&-kashmir', 'jammu & kashmir')
map_trans['State']=map_trans['State'].str.replace('dadra-&-nagar-haveli-&-daman-&-diu','dadra and nagar haveli and daman and diu')
map_trans['State']=map_trans['State'].str.replace("-"," ")
map_trans['State']=map_trans['State'].str.title()


# map user

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

map_users['State']=map_users['State'].str.replace('andaman-&-nicobar-islands','andaman & nicobar')
map_users['State']=map_users['State'].str.replace('jammu-&-kashmir', 'jammu & kashmir')
map_users['State']=map_users['State'].str.replace('dadra-&-nagar-haveli-&-daman-&-diu','dadra and nagar haveli and daman and diu')
map_users['State']=map_users['State'].str.replace("-"," ")
map_users['State']=map_users['State'].str.title()

# top transaction

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

top_trans['State']=top_trans['State'].str.replace('andaman-&-nicobar-islands','andaman & nicobar')
top_trans['State']=top_trans['State'].str.replace('jammu-&-kashmir', 'jammu & kashmir')
top_trans['State']=top_trans['State'].str.replace('dadra-&-nagar-haveli-&-daman-&-diu','dadra and nagar haveli and daman and diu')
top_trans['State']=top_trans['State'].str.replace("-"," ")
top_trans['State']=top_trans['State'].str.title()


# top user

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


top_user = pd.DataFrame(col5)

top_user['State']=top_user['State'].str.replace('andaman-&-nicobar-islands','andaman & nicobar')
top_user['State']=top_user['State'].str.replace('jammu-&-kashmir', 'jammu & kashmir')
top_user['State']=top_user['State'].str.replace('dadra-&-nagar-haveli-&-daman-&-diu','dadra and nagar haveli and daman and diu')
top_user['State']=top_user['State'].str.replace("-"," ")
top_user['State']=top_user['State'].str.title()

conn = psycopg2.connect(host="localhost",
                        user="postgres",
                        password="magu",
                        database="phonepe",
                        port="5432")
cursor = conn.cursor()

create_query_1 = """CREATE TABLE IF NOT EXISTS Agg_Tran (
                        State VARCHAR(100),
                        Year INT,
                        Quater INT,
                        Transaction_type VARCHAR(100),
                        Transaction_count INT,
                        Transaction_amount INT
                    )"""


# Create table for Agg_Tran
                                                                       
cursor.execute(create_query_1)
conn.commit()           

for index , row in Agg_Tran.iterrows():
    insert_query = """INSERT INTO Agg_Tran (State,Year,Quater,Transaction_type,Transaction_count,Transaction_amount) VALUES (%s,%s,%s,%s,%s,%s)""" 
    
    values =(row["State"],
                row["Year"],
                row["Quater"],
                row["Transaction_type"],
                row["Transaction_count"],
                row["Transaction_amount"])
                                                                                                                
    try:
        cursor.execute(insert_query, values)
        conn.commit()
    except Exception as e:
        conn.rollback()  # Rollback changes if an exception occurs
        print(f"Error inserting row {index}: {e}")

# Close cursor and connection
cursor.close()
conn.close()

# Create table for Agg_user

conn = psycopg2.connect(host="localhost",
                        user="postgres",
                        password="magu",
                        database="phonepe",
                        port="5432")
cursor = conn.cursor()

create_query_2 = """CREATE TABLE IF NOT EXISTS Agg_user(
                        State VARCHAR(100),
                        Year INT,
                        Quarter INT,
                        Transaction_brand VARCHAR(100),
                        Transaction_count INT,
                        Transaction_percentage float
                    )"""
                    
cursor.execute(create_query_2)
conn.commit()           

for index , row in Agg_user.iterrows():
    insert_query = """INSERT INTO Agg_user (State,Year,Quarter,Transaction_brand,Transaction_count,Transaction_percentage) 
    VALUES (%s,%s,%s,%s,%s,%s)""" 
    
    values =(row["State"],
                row["Year"],
                row["Quarter"],
                row["Transaction_brand"],
                row["Transaction_count"],
                row["Transaction_percentage"])
    
data=Agg_user.values.tolist()
cursor.executemany(insert_query,data)
conn.commit()                                                      
   
# create table for map_trans

conn = psycopg2.connect(host="localhost",
                        user="postgres",
                        password="magu",
                        database="phonepe",
                        port="5432")
cursor = conn.cursor()

create_query_3 = """CREATE TABLE IF NOT EXISTS map_trans (
                        State VARCHAR(100),
                        Year INT,
                        Quarter INT,
                        Transaction_type VARCHAR(100),
                        Transaction_district VARCHAR(100),
                        Transaction_count BIGINT,
                        Transaction_amount BIGINT
                    )"""


                    
cursor.execute(create_query_3)
conn.commit()           

for index , row in map_trans.iterrows():
    insert_query = """INSERT INTO map_trans (State,Year,Quarter,Transaction_type ,Transaction_district,
                        Transaction_count ,
                        Transaction_amount ) 
    VALUES (%s,%s,%s,%s,%s,%s,%s)""" 
    
    values =(row["State"],
                row["Year"],
                row["Quarter"],
                row["Transaction_type"],
                row["Transaction_district"],
                row["Transaction_count"],
                row["Transaction_amount"])
    
    try:
        cursor.execute(insert_query, values)
        conn.commit()
    except Exception as e:
        conn.rollback()  # Rollback changes if an exception occurs
        print(f"Error inserting row {index}: {e}")

# Close cursor and connection
cursor.close()
conn.close()    

# create table for map_users

conn = psycopg2.connect(host="localhost",
                        user="postgres",
                        password="magu",
                        database="phonepe",
                        port="5432")
cursor = conn.cursor()

create_query_4 = """CREATE TABLE IF NOT EXISTS map_users (
                        State VARCHAR(100),
                        Year INT,
                        Quarter INT,
                        District VARCHAR(100),
                        Registered_user BIGINT,
                        App_opening BIGINT
                    )"""

                    
cursor.execute(create_query_4)
conn.commit()
                    
for index , row in map_users.iterrows():
    insert_query = """INSERT INTO map_users (State,Year,Quarter,District,Registered_user,App_opening) 
    VALUES (%s,%s,%s,%s,%s,%s)""" 
    
    values =(row["State"],
                row["Year"],
                row["Quater"],
                row["District"],
                row["Registered_user"],
                row["App_opening"])
    
data=map_users.values.tolist()
cursor.executemany(insert_query,data)
conn.commit()                 

# create table for top_trans

conn = psycopg2.connect(host="localhost",
                        user="postgres",
                        password="magu",
                        database="phonepe",
                        port="5432")
cursor = conn.cursor()

create_query_5 = """CREATE TABLE IF NOT EXISTS top_trans (
                        State VARCHAR(100),
                        Year INT,
                        Quarter INT,
                        Transaction_type VARCHAR(100),
                        Transaction_district VARCHAR(100),
                        Transaction_count BIGINT,
                        Transaction_amount BIGINT
                    )"""
                    
cursor.execute(create_query_5)
conn.commit()

for index , row in top_trans.iterrows():
    insert_query = """INSERT INTO top_trans (State,Year,Quarter,Transaction_type ,Transaction_district,
                        Transaction_count ,
                        Transaction_amount ) 
    VALUES (%s,%s,%s,%s,%s,%s,%s)""" 
    
    values =(row["State"],
                row["Year"],
                row["Quarter"],
                row["Transaction_type"],
                row["Transaction_district"],
                row["Transaction_count"],
                row["Transaction_amount"])
    
data=top_trans.values.tolist()
cursor.executemany(insert_query,data)
conn.commit()                    

# Create table for top_user

conn = psycopg2.connect(host="localhost",
                        user="postgres",
                        password="magu",
                        database="phonepe",
                        port="5432")
cursor = conn.cursor()

create_query_6 = """CREATE TABLE IF NOT EXISTS  top_users (
                        State VARCHAR(100),
                        Year INT,
                        Quarter INT,
                        registeredusers BIGINT,
                        Transaction_district VARCHAR(100)
                    )"""
                    
                    
cursor.execute(create_query_6)
conn.commit()

for index , row in top_users.iterrows():
    insert_query = """INSERT INTO top_users (State,Year,Quarter,registeredusers,Transaction_district)
    VALUES (%s,%s,%s,%s,%s)"""
    
    values =(row["State"],
                row["Year"],
                row["Quarter"],
                row["registeredusers"],
                row["Transaction_district"])
    
data=top_users.values.tolist()
cursor.executemany(insert_query,data)
conn.commit()

                 
