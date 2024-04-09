import pandas as pd
import psycopg2
import json
import os
import requests
import plotly.express as px
import plotly.io as pio
import streamlit as st
from streamlit_option_menu import option_menu
from sqlalchemy import engine
from sqlalchemy import create_engine
from PIL import Image

# Set up connection to PostgreSQL database

engine = create_engine('postgresql+psycopg2://postgres:magu@localhost/phonepe')

agg_tran_df = pd.read_sql('agg_tran', engine)
agg_user_df = pd.read_sql('agg_user', engine)
map_trans_df = pd.read_sql('map_trans', engine)
map_users_df = pd.read_sql('map_users', engine)
top_trans_df = pd.read_sql('top_trans', engine)
top_user_df = pd.read_sql('top_user', engine)




def transaction_amount_count(df,years):
    
    agg_tran_df=df[ df["year"] == years]
    agg_tran_df.reset_index(drop=True, inplace=True)
    
    agg_trans_group = agg_tran_df.groupby('states')[['transaction_amount','transaction_count']].sum()
    agg_trans_group.reset_index(inplace=True)
    
    col1,col2 = st.columns(2)
    with col1:
    
        fig_amount= px.bar(agg_trans_group, x="states", y= "transaction_amount",title= f"{years} TRANSACTION AMOUNT",
                                color_discrete_sequence=px.colors.sequential.Aggrnyl,height= 650,width = 600)
        
        st.plotly_chart(fig_amount)
    
    with col2:
        
        fig_count= px.bar(agg_trans_group, x="states", y= "transaction_count",title= f"{years} TRANSACTION COUNT",
                                color_discrete_sequence=px.colors.sequential.Bluered_r,height= 650,width = 600)
        
        st.plotly_chart(fig_count)
    
    
    col1,col2 = st.columns(2)
    
    with col1:
        url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
        response = requests.get(url)
        data1=json.loads(response.content)
        state_name = []
        for feature in data1['features']:
            state_name.append(feature['properties']["ST_NM"])
            
        state_name.sort()

        fig_map_1=px.choropleth(agg_trans_group,geojson=data1,locations="states",featureidkey="properties.ST_NM",
                                color="transaction_amount",color_continuous_scale="ylgnbu",
                                range_color=(agg_tran_df["transaction_amount"].min(),agg_tran_df["transaction_amount"].max()),
                                hover_name ="states",title= f"{years} TRANSACTION AMOUNT",fitbounds="locations",
                                height= 650,width = 600)
        fig_map_1.update_geos(visible = False)
        
        st.plotly_chart(fig_map_1)
        
    with col2:   
    
        fig_map_2=px.choropleth(agg_trans_group,geojson=data1,locations="states",featureidkey="properties.ST_NM",
                            color="transaction_count",color_continuous_scale="ylorrd",
                            range_color=(agg_tran_df["transaction_count"].min(),agg_tran_df["transaction_count"].max()),
                            hover_name ="states",title= f"{years} TRANSACTION COUNT",fitbounds="locations",
                            height= 650,width = 600)
        fig_map_2.update_geos(visible = False)

        st.plotly_chart(fig_map_2)
        
    return agg_tran_df




def transaction_amount_count_quarter(df,quarter):
    
    agg_tran_df=df[ df["quarter"] == quarter]
    agg_tran_df.reset_index(drop=True, inplace=True)
    
    agg_trans_group = agg_tran_df.groupby('states')[['transaction_amount','transaction_count']].sum()
    agg_trans_group.reset_index(inplace=True)
    
    col1,col2 = st.columns(2)
    
    with col1:
    
        fig_amount= px.bar(agg_trans_group, x="states", y= "transaction_amount",title= f"{agg_tran_df['year'].min()} years {quarter} quarter TRANSACTION AMOUNT",
                                color_discrete_sequence=px.colors.sequential.Aggrnyl,height= 650,width = 600)
        
        st.plotly_chart(fig_amount)
    
    with col2:
        
        fig_count= px.bar(agg_trans_group, x="states", y= "transaction_count",title=  f"{agg_tran_df['year'].min()} years {quarter} quarter  TRANSACTION COUNT",
                                color_discrete_sequence=px.colors.sequential.Bluered_r,height= 650,width = 600)
        
        st.plotly_chart(fig_count)
    
    col1,col2 = st.columns(2)
    
    with col1:
    
        url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
        response = requests.get(url)
        data1=json.loads(response.content)
        state_name = []
        for feature in data1['features']:
            state_name.append(feature['properties']["ST_NM"])
            
        state_name.sort()

        fig_map_1=px.choropleth(agg_trans_group,geojson=data1,locations="states",featureidkey="properties.ST_NM",
                                color="transaction_amount",color_continuous_scale="ylgnbu",
                                range_color=(agg_tran_df["transaction_amount"].min(),agg_tran_df["transaction_amount"].max()),
                                hover_name ="states",title=  f"{agg_tran_df['year'].min()} years {quarter} quarter  TRANSACTION AMOUNT",fitbounds="locations",
                                height= 650,width = 600)
        fig_map_1.update_geos(visible = False)
        
        st.plotly_chart(fig_map_1)
        
    with col2:
        
        fig_map_2=px.choropleth(agg_trans_group,geojson=data1,locations="states",featureidkey="properties.ST_NM",
                            color="transaction_count",color_continuous_scale="ylorrd",
                            range_color=(agg_tran_df["transaction_count"].min(),agg_tran_df["transaction_count"].max()),
                            hover_name ="states",title= f"{agg_tran_df['year'].min()} years {quarter} quarter  TRANSACTION COUNT",fitbounds="locations",
                            height= 650,width = 600)
        fig_map_2.update_geos(visible = False)

        st.plotly_chart(fig_map_2)
    
    return agg_tran_df


    

def map_amount_count_quarter(df,quarter):
    
    agg_tran_df=df[ df["quarter"] == quarter]
    agg_tran_df.reset_index(drop=True, inplace=True)
    
    agg_trans_group = agg_tran_df.groupby('states')[['transaction_amount','transaction_count']].sum()
    agg_trans_group.reset_index(inplace=True)
    
    col1,col2 = st.columns(2)
    
    with col1:
    
        fig_amount= px.bar(agg_trans_group, x="states", y= "transaction_amount",title= f"{agg_tran_df['year'].min()} years {quarter} quarter TRANSACTION AMOUNT",
                                color_discrete_sequence=px.colors.sequential.Aggrnyl,height= 650,width = 600)
        
        st.plotly_chart(fig_amount)
    
    with col2:
    
        fig_count= px.bar(agg_trans_group, x="states", y= "transaction_count",title=  f"{agg_tran_df['year'].min()} years {quarter} quarter  TRANSACTION COUNT",
                                color_discrete_sequence=px.colors.sequential.Bluered_r,height= 650,width = 600)
        
        st.plotly_chart(fig_count)
        




def transaction_amount_type(df,state):
    
    agg_tran_df=df[ df["states"] == state]
    agg_tran_df.reset_index(drop=True, inplace=True)
    
    agg_trans_group = agg_tran_df.groupby('transaction_type')[['transaction_amount','transaction_count']].sum()
    agg_trans_group.reset_index(inplace=True)
    
    col1,col2 = st.columns(2)
    
    with col1:
    
        fig_pie_1= px.pie (agg_trans_group,values = "transaction_amount",names = "transaction_type",
                            title = f"{state.upper()} TRANSACTION AMOUNT",hole = 0.5)
        
        st.plotly_chart(fig_pie_1)
    
    with col2:
    
        fig_pie_2= px.pie (agg_trans_group,values = "transaction_count",names = "transaction_type",
                            title = f"{state.upper()} TRANSACTION COUNT",hole = 0.5)
        
        st.plotly_chart(fig_pie_2)
        
        
        
        
    
def agg_user_type(df,years):
    agg_user_year=df[df["year"] == years]
    agg_user_year.reset_index(drop=True, inplace=True)
    

    agg_user_year_group = agg_user_year.groupby('transaction_brand')[['transaction_count']].sum()
    agg_user_year_group.reset_index(inplace=True)
     

    fig_bar_1 = px.bar(agg_user_year_group, x="transaction_brand", y= "transaction_count",title= f"{years} BRANDS AND TRANSACTION COUNT",
                    width= 1000,color_discrete_sequence=px.colors.sequential.haline,hover_name= "transaction_brand",height= 500)
    st.plotly_chart(fig_bar_1)
    
    return agg_user_year




# agg _user year quarter

def agg_user_year_quarter(df,quarter):
    agg_user_quarter= df[df["quarter"] == quarter]
    agg_user_quarter.reset_index(drop=True, inplace=True)

    agg_user_quarter_group = agg_user_quarter.groupby('transaction_brand')[['transaction_count']].sum()
    agg_user_quarter_group.reset_index(inplace=True)

    fig_bar_2 = px.bar(agg_user_quarter_group, x="transaction_brand", y= "transaction_count",title=f"{quarter} quarter BRANDS AND TRANSACTION COUNT",
                    width= 1000,color_discrete_sequence=px.colors.sequential.Plasma_r,hover_name= "transaction_brand",height= 500)
    st.plotly_chart(fig_bar_2)
    
    return agg_user_quarter




# agg _user year quarter

def agg_user2(df,state):

    aggr_quarter_state =df[df["states"] == state]
    aggr_quarter_state.reset_index(drop=True, inplace=True)

    fig_line_1 = px.line(aggr_quarter_state, x="transaction_brand", y="transaction_count",hover_data="transaction_percentage",
                        title= f"{state.upper()}  BRANDS-TRANSACTION COUNT-PERCENTAGE",width= 1000,markers = True)
    st.plotly_chart(fig_line_1)  
    
      



def map_trans_district_1(df,state):
    
    map_trans_df=df[ df["states"] == state]
    map_trans_df.reset_index(drop=True, inplace=True)
    
    map_trans_group = map_trans_df.groupby('transaction_district')[['transaction_amount','transaction_count']].sum()
    map_trans_group.reset_index(inplace=True)
    
    col1,col2 = st.columns(2)
    
    with col1:
    
        fig_amount= px.bar(map_trans_group,x = "transaction_amount",y= "transaction_district",orientation = "h",height=800,
                            title = f"{state.upper()} district-WISE TRANSACTION AMOUNT",
                            color_discrete_sequence=px.colors.sequential.Bluered_r)
        
        st.plotly_chart(fig_amount) 
            
    with col2:
    
        fig_count= px.bar (map_trans_group,x = "transaction_count",y = "transaction_district",orientation = "h",height=800,
                            title = f"{state.upper()} district-WISE TRANSACTION COUNT",
                            color_discrete_sequence=px.colors.sequential.haline)
        
        st.plotly_chart(fig_count)
        
        


# map_user_plot_1

def map_user_plot_1(df,years):

    map_user_year=df[df["year"] == years]
    map_user_year.reset_index(drop=True, inplace=True)

    map_user_year_group = map_user_year.groupby('states')[['registered_user','app_opening']].sum()
    map_user_year_group.reset_index(inplace=True)


    fig_line_1 = px.line(map_user_year_group, x="states", y=["registered_user","app_opening"],
                            title= f"{years} REGESTERED USERS AND APP OPENS",width= 1000,height= 700,markers = True)
    st.plotly_chart(fig_line_1)
    
    return map_user_year




# map_user_plot_1

def map_user_plot_2(df,quarter):

    map_user_year_q=df[df["quarter"] == quarter]
    map_user_year_q.reset_index(drop=True, inplace=True)

    map_user_quarter_group = map_user_year_q.groupby('states')[['registered_user','app_opening']].sum()
    map_user_quarter_group.reset_index(inplace=True)


    fig_line_1 = px.line(map_user_quarter_group , x="states", y=["registered_user","app_opening"],
                            title= f"{df['year'].min()}  {quarter}  REGESTERED USERS AND APP OPENS",width= 1000,height= 700,markers = True,
                            color_discrete_sequence=px.colors.sequential.Rainbow_r)
    st.plotly_chart(fig_line_1)
    
    return map_user_year_q





def map_user_district_1(df,state):

    map_user_quarter= df[df["states"] == state]
    map_user_quarter.reset_index(drop=True, inplace=True)
    
    col1,col2 = st.columns(2)
    
    with col1:

        fig_map_user_1 = px.bar (map_user_quarter, x="registered_user", y="district", orientation="h",height=800,
                                title= f"{state.upper()} REGISTERED USERS",color_discrete_sequence=px.colors.sequential.haline)

        st.plotly_chart(fig_map_user_1)
        
    with col2:    

        fig_map_user_2 = px.bar (map_user_quarter, x="app_opening", y="district", orientation="h",height=800,
                                title= f"{state.upper()} APP OPENS",color_discrete_sequence=px.colors.sequential.Aggrnyl)

        st.plotly_chart(fig_map_user_2)
        
        


def top_user_plot_1(df,years):

    top_user_year=df[df["year"] == years]
    top_user_year.reset_index(drop=True, inplace=True)


    top_user_year_group = top_user_year.groupby(['states','quarter'])[['registeredusers']].sum()
    top_user_year_group.reset_index(inplace=True)

    fig_top_user=px.bar(top_user_year_group, x="states", y="registeredusers",color="quarter",width=1000, height=800,hover_name="states",
                        color_discrete_sequence=px.colors.sequential.Burgyl,
                        title=f"{years} year TOP states AND REGISTERED USERS")
    st.plotly_chart(fig_top_user)
    
    return top_user_year




def top_user_plot_2(df,state):

    top_user_state=df [df ["states"] == state]
    top_user_state.reset_index(drop=True, inplace=True)

    top_user_state_group = px.bar(top_user_state, x="quarter", y= "registeredusers",title= f"{state} REGISTERED USERS",color="registeredusers",
                                hover_data="transaction_district",width= 800,
                                height= 700, color_continuous_scale=px.colors.sequential.haline)
    st.plotly_chart(top_user_state_group)
    
    
 
def top_charts_transaction(table_name):


    mydb = psycopg2.connect(host="localhost",
                            user="postgres",
                            password="magu",
                            database="phonepe",
                            port="5432")
    cursor = mydb.cursor()

    # QUERY 1

    query1=f'''select states, sum (transaction_amount) as transaction_amount
                                    from {table_name} 
                                    group by states 
                                    order by transaction_amount desc
                                    limit 10 ;'''
                                    
                                    
    cursor.execute(query1)
    table1= cursor.fetchall()
    mydb.commit()
    
    col1,col2 = st.columns(2)
    with col1:

        data_frame_1 = pd.DataFrame(table1,columns=['states','transaction_amount'])

        fig_amount1= px.bar(data_frame_1, x="states", y= "transaction_amount",title= "TOP 10 TRANSACTION AMOUNT",hover_name="states",
                            color_discrete_sequence=px.colors.sequential.Aggrnyl,height= 500,width = 400)

        st.plotly_chart(fig_amount1)

    # QUERY 2


    query2=f'''select states, sum (transaction_amount) as transaction_amount
                                    from {table_name} 
                                    group by states 
                                    order by transaction_amount 
                                    limit 10 ;'''
                                    
                                    
    cursor.execute(query2)
    table2= cursor.fetchall()
    mydb.commit()

    data_frame_2 = pd.DataFrame(table2,columns=['states','transaction_amount'])
    
    with col2:
    
        fig_amount2= px.bar(data_frame_2, x="states", y= "transaction_amount",title= "LEAST 10 TRANSACTION AMOUNT",hover_name="states",
                            
                            color_discrete_sequence=px.colors.sequential.Aggrnyl_r,height= 600,width = 400)

        st.plotly_chart(fig_amount2)

    # QUERY 3


    query3=f'''select states, avg (transaction_amount) as transaction_amount
                                    from {table_name} 
                                    group by states 
                                    order by transaction_amount  ;'''
                                    
                                    
    cursor.execute(query3)
    table3= cursor.fetchall()
    mydb.commit()

    data_frame_3 = pd.DataFrame(table3,columns=['states','transaction_amount'])

    fig_amount3= px.bar(data_frame_3, x="states", y= "transaction_amount",title= "AVERAGE TRANSACTION AMOUNT",hover_name="states",
                        
                        color_discrete_sequence=px.colors.sequential.Bluered_r,height= 650,width = 800)

    st.plotly_chart(fig_amount3)
                


def top_charts_transaction_count(table_name):


    mydb = psycopg2.connect(host="localhost",
                            user="postgres",
                            password="magu",
                            database="phonepe",
                            port="5432")
    cursor = mydb.cursor()

    # QUERY 1

    query1=f'''select states, sum (transaction_count) as transaction_count
                                    from {table_name} 
                                    group by states 
                                    order by transaction_count desc
                                    limit 10 ;'''
                                    
                                    
    cursor.execute(query1)
    table1= cursor.fetchall()
    mydb.commit()
    
    col1,col2 = st.columns(2)
    
    with col1:

        data_frame_1 = pd.DataFrame(table1,columns=['states','transaction_count'])

        fig_amount1= px.line(data_frame_1, x="states", y= "transaction_count",title= "TOP 10 TRANSACTION COUNT",
                            hover_name="states",markers = True,
                            color_discrete_sequence=px.colors.sequential.Aggrnyl,height= 600,width = 500)

        st.plotly_chart(fig_amount1)

    # QUERY 2


    query2=f'''select states, sum (transaction_count) as transaction_count
                                    from {table_name} 
                                    group by states 
                                    order by transaction_count 
                                    limit 10 ;'''
                                    
                                    
    cursor.execute(query2)
    table2= cursor.fetchall()
    mydb.commit()
    
    with col2:

        data_frame_2 = pd.DataFrame(table2,columns=['states','transaction_count'])

        fig_amount2= px.line(data_frame_2, x="states", y= "transaction_count",title= "LEAST 10 TRANSACTION COUNT",
                            hover_name="states",markers = True,
                            
                            color_discrete_sequence=px.colors.sequential.Aggrnyl_r,height= 600,width = 500)

        st.plotly_chart(fig_amount2)

    # QUERY 3


    query3=f'''select states, avg (transaction_count) as transaction_count
                                    from {table_name} 
                                    group by states 
                                    order by transaction_count  ;'''
                                    
                                    
    cursor.execute(query3)
    table3= cursor.fetchall()
    mydb.commit()

    data_frame_3 = pd.DataFrame(table3,columns=['states','transaction_count'])

    fig_amount3= px.bar(data_frame_3, x="states", y= "transaction_count",title= "AVERAGE TRANSACTION COUNT",
                        hover_name="states",
                        
                        color_discrete_sequence=px.colors.sequential.Bluered_r,height= 650,width = 800)

    st.plotly_chart(fig_amount3)
             
             
def top_charts_registers(state):

    mydb = psycopg2.connect(host="localhost",
                            user="postgres",
                            password="magu",
                            database="phonepe",
                            port="5432")
    cursor = mydb.cursor()

    # QUERY 1

    query9=f'''select district, sum (registered_user) as registered_user
                                from  map_users
                                where states='{state}'
                                group by district 
                                order by registered_user desc
                                limit 10 ;'''
                                    
                                    
                                    
    cursor.execute(query9)
    table1= cursor.fetchall()
    mydb.commit()
    
    col1,col2 = st.columns(2)
    with col1:

        data_frame_1 = pd.DataFrame(table1,columns=['district','registered_user'])

        fig_amount1= px.bar(data_frame_1, x="district", y= "registered_user",title= "TOP 10 REGISTERED USERS",
                            color_discrete_sequence=px.colors.sequential.Aggrnyl,height= 500,width = 500)

        st.plotly_chart(fig_amount1)

    # QUERY 2


    query2=f'''select district, sum (registered_user) as registered_user
                            from  map_users
                            where states='{state}'
                            group by district 
                            order by registered_user 
                            limit 10 ;'''
                                    
                                    
    cursor.execute(query2)
    table2= cursor.fetchall()
    mydb.commit()
    
    with col2:

        data_frame_2 = pd.DataFrame(table2,columns=['district','registered_user'])

        fig_amount2= px.bar(data_frame_2, x="district", y= "registered_user",title= "LEAST 10 REGISTERED USERS",
                            
                            color_discrete_sequence=px.colors.sequential.Aggrnyl_r,height= 500,width = 500)

        st.plotly_chart(fig_amount2)

    # QUERY 3


    query3=f'''select district, sum (registered_user) as registered_user
                            from  map_users
                            where states='{state}'
                            group by district 
                            order by registered_user ;'''
                                    
                                    
    cursor.execute(query3)
    table3= cursor.fetchall()
    mydb.commit()

    data_frame_3 = pd.DataFrame(table3,columns=['district','registered_user'])

    fig_amount3= px.bar(data_frame_3, x="district", y= "registered_user",title= "AVERAGE OF REGISTERED USERS",
                        
                        color_discrete_sequence=px.colors.sequential.Bluered_r,height= 800,width = 1000)
    st.plotly_chart(fig_amount3)    
    
    
def top_charts_registers2(state):

    mydb = psycopg2.connect(host="localhost",
                            user="postgres",
                            password="magu",
                            database="phonepe",
                            port="5432")
    cursor = mydb.cursor()

    # QUERY 1

    query9=f'''select transaction_district, sum (registeredusers) as registeredusers
                                from  top_user
                                where states='{state}'
                                group by transaction_district 
                                order by registeredusers desc
                                limit 10 ;'''
                                    
                                    
                                    
    cursor.execute(query9)
    table1= cursor.fetchall()
    mydb.commit()
    
    col1,col2 = st.columns(2)
    with col1:

        data_frame_1 = pd.DataFrame(table1,columns=['transaction_district','registeredusers'])

        fig_amount1= px.bar(data_frame_1, x="transaction_district", y= "registeredusers",title= "TOP 10 REGISTERED USERS",
                            color_discrete_sequence=px.colors.sequential.Aggrnyl,height= 500,width = 500)

        st.plotly_chart(fig_amount1)

    # QUERY 2


    query2=f'''select transaction_district, sum (registeredusers) as registeredusers
                            from  top_user
                            where states='{state}'
                            group by transaction_district 
                            order by registeredusers 
                            limit 10 ;'''
                                    
                                    
    cursor.execute(query2)
    table2= cursor.fetchall()
    mydb.commit()
    
    with col2:

        data_frame_2 = pd.DataFrame(table2,columns=['transaction_district','registeredusers'])

        fig_amount2= px.bar(data_frame_2, x="transaction_district", y= "registeredusers",title= "LEAST 10 REGISTERED USERS",
                            
                            color_discrete_sequence=px.colors.sequential.Aggrnyl_r,height= 500,width = 500)

        st.plotly_chart(fig_amount2)

    # QUERY 3


    query3=f'''select transaction_district, sum (registeredusers) as registeredusers
                            from  top_user
                            where states='{state}'
                            group by transaction_district 
                            order by registeredusers ;'''
                                    
                                    
    cursor.execute(query3)
    table3= cursor.fetchall()
    mydb.commit()

    data_frame_3 = pd.DataFrame(table3,columns=['transaction_district','registeredusers'])

    fig_amount3= px.bar(data_frame_3, x="transaction_district", y= "registeredusers",title= "AVERAGE OF REGISTERED USERS",
                        
                        color_discrete_sequence=px.colors.sequential.Bluered_r,height= 500,width = 1000)
    st.plotly_chart(fig_amount3)       
             


def top_charts_appopens(state):

    mydb = psycopg2.connect(host="localhost",
                            user="postgres",
                            password="magu",
                            database="phonepe",
                            port="5432")
    cursor = mydb.cursor()

    # QUERY 1

    query9=f'''select district, sum (app_opening) as app_opening
                                from  map_users
                                where states='{state}'
                                group by district 
                                order by app_opening desc
                                limit 10 ;'''
                                    
                                    
                                    
    cursor.execute(query9)
    table1= cursor.fetchall()
    mydb.commit()
    
    col1,col2 = st.columns(2)
    with col1:

        data_frame_1 = pd.DataFrame(table1,columns=['district','app_opening'])

        fig_amount1= px.bar(data_frame_1, x="district", y= "app_opening",title= "TOP 10 APPOPENS",
                            color_discrete_sequence=px.colors.sequential.Aggrnyl,height= 500,width = 500)

        st.plotly_chart(fig_amount1)

    # QUERY 2


    query2=f'''select district, sum (app_opening) as app_opening
                            from  map_users
                            where states='{state}'
                            group by district 
                            order by app_opening 
                            limit 10 ;'''
                                    
                                    
    cursor.execute(query2)
    table2= cursor.fetchall()
    mydb.commit()
    
    with col2:

        data_frame_2 = pd.DataFrame(table2,columns=['district','app_opening'])

        fig_amount2= px.bar(data_frame_2, x="district", y= "app_opening",title= "LEAST 10 APPOPENS",
                            
                            color_discrete_sequence=px.colors.sequential.Aggrnyl_r,height= 500,width = 500)

        st.plotly_chart(fig_amount2)

    # QUERY 3


    query3=f'''select district, sum (app_opening) as app_opening
                            from  map_users
                            where states='{state}'
                            group by district 
                            order by app_opening ;'''
                                    
                                    
    cursor.execute(query3)
    table3= cursor.fetchall()
    mydb.commit()

    data_frame_3 = pd.DataFrame(table3,columns=['district','app_opening'])

    fig_amount3= px.bar(data_frame_3, x="district", y= "app_opening",title= "AVERAGE OF APPOPENS",
                        
                        color_discrete_sequence=px.colors.sequential.Bluered_r,height= 800,width = 1000)
    st.plotly_chart(fig_amount3)  
    
    
    
    
    
     
    
    

# streamlit code

st.set_page_config(layout= "wide")
st.title(':violet[Phonepe Pulse Data Visualization and Exploration]')

with  st.sidebar:
    select=option_menu("Main Menu",["Home","Data Exploration","Top Charts"],)
    
    


if  select=="Home":
    col1,col2=st.columns(2)
    with col1:
        st.header("Phonepe Pulse Data Visualization and Exploration")
        
        
        st.header(":red[What We Do:]")
        st.write("**We look at how people use Phonepe to send money and buy things. Then, we make charts which helps you to understand what's happening.**")

        st.header(":red[Where We Get Our Information:]")
        st.write("We get our information from Phonepe's github.")
        
    with col2:
        st.image(Image.open("E:\VS CODE files\.venv\PhonePe first fintech firm to allow international payments1675763759900.jpg"),width=700)
        
    col1,col2=st.columns(2)
    with col1:    

        st.header(":red[Special Things to See:]")
        st.write("You can find out where people buy the most in different places, or how many people use Phonepe each year & quarter.")

        st.header(":red[How to Use:]")
        st.write("You can pick which place, year, or thing you want to see more about. Just click on the sidebar to learn more.")
    with col2:
        st.image(Image.open("E:\VS CODE files\.venv\images (1).png"),width=700)

    

elif select=="Data Exploration":
    
    tab1, tab2, tab3 = st.tabs(["Aggregated analysis","Map analysis","Top analysis"])
    
    
    

    with tab1:
        method_1 = st.radio("select the Aggregate type",["Aggregate Transaction","Aggregate User"])
        
        if method_1 == "Aggregate Transaction":
            
            col1,col2=st.columns(2)
            with col1:
                years=st.slider("select the year",agg_tran_df["year"].min(),agg_tran_df["year"].max(),
                                agg_tran_df["year"].min())
            tran_q  = transaction_amount_count(agg_tran_df,years)  
              
              
            
            col1,col2 = st.columns(2)
            with col1:
                states=st.selectbox("select the state_quarter",tran_q ["states"].unique())
                
            transaction_amount_type(tran_q,states)    
            
            
                
            col1,col2 = st.columns(2)
            with col1:
                quarters = st.slider("select the quarter",tran_q["quarter"].min(),tran_q["quarter"].max(),
                                     tran_q["quarter"].min())
            tran_qater = transaction_amount_count_quarter(tran_q,quarters)
            
            
            
                
            
        elif  method_1 == "Aggregate User":
            
            col1,col2=st.columns(2)
            with col1:
                
                years=st.slider("select the year",agg_user_df["year"].min(),agg_user_df["year"].max(),agg_user_df["year"].min())
            user_year=agg_user_type(agg_user_df,years)
            
            
            
            col1,col2=st.columns(2)
            with col1:
                quarters = st.slider("select the quarter",user_year["quarter"].min(),user_year["quarter"].max(),
                                     user_year["quarter"].min())
            user_year_qater = agg_user_year_quarter(user_year,quarters)
            
            
            
            col1,col2 = st.columns(2)
            with col1:
                states=st.selectbox("select the state_user",user_year_qater["states"].unique())
                
            agg_user2(user_year_qater,states)  
            
            
            
    
    with tab2:
        method_2= st.radio("select the map type",["Map Transaction","Map User"])
        
        if  method_2 == "Map Transaction":
            
            
            col1,col2=st.columns(2)
            with col1:
                years=st.slider("select the year_map",map_trans_df["year"].min(),map_trans_df["year"].max(),
                                map_trans_df["year"].min())
            map_transac  = transaction_amount_count(map_trans_df,years) 
            
            
                
            col1,col2 = st.columns(2)
            with col1:
                states=st.selectbox("select state_map",map_transac ["states"].unique())
                
            map_trans_district_1(map_transac,states) 
            
            
            
            col1,col2 = st.columns(2)
            with col1:
                quarters = st.slider("select the quarter_map",map_transac["quarter"].min(),map_transac["quarter"].max(),
                                     map_transac["quarter"].min())
            map_qater = map_amount_count_quarter(map_transac,quarters) 
            
            
        elif method_2 == "Map User":
            
            
            col1,col2=st.columns(2)
            with col1:
                years=st.slider("select the year_map_user",map_users_df["year"].min(),map_users_df["year"].max(),
                                map_users_df["year"].min())
            map_user_year  = map_user_plot_1(map_users_df,years)
            
            
            
            col1,col2=st.columns(2)
            with col1:
                quarters = st.slider("select the quarter_map_user",map_user_year["quarter"].min(),map_user_year["quarter"].max(),
                                     map_user_year["quarter"].min())
            map_year_qater = map_user_plot_2(map_user_year,quarters)
            
            
            
            col1,col2 = st.columns(2)
            with col1:
                states=st.selectbox("select the state_map_user",map_year_qater ["states"].unique())
            map_user_district_1(map_year_qater,states)
            
            

    with tab3:
        method_3= st.radio("select the top type",["Top Transactions","Top Users"])
        
        if  method_3 =="Top Transactions":
            
            
            col1,col2=st.columns(2)
            with col1:
                years=st.slider("select the year_top",top_trans_df["year"].min(),top_trans_df["year"].max(),
                                top_trans_df["year"].min())
            top_transac  = transaction_amount_count(top_trans_df,years)
            
            
            
            col1,col2=st.columns(2)
            with col1:
                quarters = st.slider("select the quarter_top",top_transac ["quarter"].min(),top_transac ["quarter"].max(),
                                     top_transac ["quarter"].min())
            top_year_qater =map_amount_count_quarter(top_transac ,quarters)
            
            
        elif  method_3 =="Top Users":
            
            
            col1,col2=st.columns(2)
            with col1:
                years=st.slider("select the year_top",top_user_df["year"].min(),top_user_df["year"].max(),
                                top_user_df["year"].min())
            top_user_1 = top_user_plot_1(top_user_df,years)
            
            
            
            col1,col2 = st.columns(2)
            with col1:
                states=st.selectbox("select state_top_user",top_user_1 ["states"].unique())
                
            top_user_plot_2(top_user_1,states)
            
            
        
elif  select=="Top Charts":
    
    question = st.selectbox("SELECT THE QUESTIONS",["1.Aggregated Transaction Amount",
                                                    "2.Map Transaction Amount",
                                                    "3.Top Transaction Amount",
                                                    "4.Aggregated Transaction Count",
                                                    "5.Map Transaction Count",
                                                    "6.Top Transaction Count",
                                                    "7.Transaction Count of Aggregated User",
                                                    "8.Registeredusers of Map users",
                                                    "9.App Opens of Map users",
                                                    "10.Registeredusers of Top users"])
    
    if question == "1.Aggregated Transaction Amount":
        
        st.subheader("Aggregated Transaction Amount")
        top_charts_transaction("agg_tran")
        
        
    if question == "2.Map Transaction Amount":
        
        st.subheader("Map Transaction Amount")
        top_charts_transaction("map_trans")
        
        
    if question == "3.Top Transaction Amount":
        
        st.subheader("Top Transaction Amount")
        top_charts_transaction("top_trans")
        
        
    if question == "4.Aggregated Transaction Count":
        
        st.subheader("Aggregated Transaction Count")
        top_charts_transaction_count("agg_tran")
        
        
    if question == "5.Map Transaction Count":
        
        st.subheader("Map Transaction Count")
        top_charts_transaction_count("map_trans")
        
        
    if question == "6.Top Transaction Count":
        
        st.subheader("Top Transaction Count")
        top_charts_transaction_count("top_trans")
        
        
    if question == "7.Transaction Count of Aggregated User":
        
        st.subheader("Aggregated user Count")
        top_charts_transaction_count("agg_user")
        
    
    if question == "8.Registeredusers of Map users":
        
        st.subheader("Registeredusers of Map")
        state=st.selectbox("select the state",map_users_df["states"].unique())
        top_charts_registers(state)
        
    
    if question == "9.App Opens of Map users":
        
        st.subheader("App Opens of user")
        state=st.selectbox("select the state",map_users_df["states"].unique())
        top_charts_appopens(state)
        
    
    if question == "10.Registeredusers of Top users":
        
        st.subheader("Registeredusers of Top")
        state=st.selectbox("select the state",top_user_df["states"].unique())
        top_charts_registers2(state)
         
    
        
