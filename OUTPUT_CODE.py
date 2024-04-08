import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
import json
import os
import requests
import plotly.express as px
import plotly.io as pio
import streamlit as st
from streamlit_option_menu import option_menu
from sqlalchemy import engine

# Set up connection to PostgreSQL database

engine = create_engine('postgresql+psycopg2://postgres:magu@localhost/phonepe')

agg_tran_df = pd.read_sql('agg_tran', engine)
agg_user_df = pd.read_sql('agg_user', engine)
map_trans_df = pd.read_sql('map_trans', engine)
map_users_df = pd.read_sql('map_users', engine)
top_trans_df = pd.read_sql('top_trans', engine)
top_user_df = pd.read_sql('top_user', engine)


def transaction_amount_count(df,years):
    
    agg_tran_df=df[ df["Year"] == years]
    agg_tran_df.reset_index(drop=True, inplace=True)
    
    agg_trans_group = agg_tran_df.groupby('State')[['Transaction_amount','Transaction_count']].sum()
    agg_trans_group.reset_index(inplace=True)
    
    col1,col2 = st.columns(2)
    with col1:
    
        fig_amount= px.bar(agg_trans_group, x="State", y= "Transaction_amount",title= f"{years} TRANSACTION AMOUNT",
                                color_discrete_sequence=px.colors.sequential.Aggrnyl,height= 650,width = 600)
        
        st.plotly_chart(fig_amount)
    
    with col2:
        
        fig_count= px.bar(agg_trans_group, x="State", y= "Transaction_count",title= f"{years} TRANSACTION COUNT",
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

        fig_map_1=px.choropleth(agg_trans_group,geojson=data1,locations="State",featureidkey="properties.ST_NM",
                                color="Transaction_amount",color_continuous_scale="ylgnbu",
                                range_color=(agg_tran_df["Transaction_amount"].min(),agg_tran_df["Transaction_amount"].max()),
                                hover_name ="State",title= f"{years} TRANSACTION AMOUNT",fitbounds="locations",
                                height= 650,width = 600)
        fig_map_1.update_geos(visible = False)
        
        st.plotly_chart(fig_map_1)
        
    with col2:   
    
        fig_map_2=px.choropleth(agg_trans_group,geojson=data1,locations="State",featureidkey="properties.ST_NM",
                            color="Transaction_count",color_continuous_scale="ylorrd",
                            range_color=(agg_tran_df["Transaction_count"].min(),agg_tran_df["Transaction_count"].max()),
                            hover_name ="State",title= f"{years} TRANSACTION COUNT",fitbounds="locations",
                            height= 650,width = 600)
        fig_map_2.update_geos(visible = False)

        st.plotly_chart(fig_map_2)
        
    return agg_tran_df


def transaction_amount_count_quarter(df,quarter):
    
    agg_tran_df=df[ df["Quater"] == quarter]
    agg_tran_df.reset_index(drop=True, inplace=True)
    
    agg_trans_group = agg_tran_df.groupby('State')[['Transaction_amount','Transaction_count']].sum()
    agg_trans_group.reset_index(inplace=True)
    
    col1,col2 = st.columns(2)
    
    with col1:
    
        fig_amount= px.bar(agg_trans_group, x="State", y= "Transaction_amount",title= f"{agg_tran_df['Year'].min()} years {quarter} QUARTER TRANSACTION AMOUNT",
                                color_discrete_sequence=px.colors.sequential.Aggrnyl,height= 650,width = 600)
        
        st.plotly_chart(fig_amount)
    
    with col2:
        
        fig_count= px.bar(agg_trans_group, x="State", y= "Transaction_count",title=  f"{agg_tran_df['Year'].min()} years {quarter} QUARTER  TRANSACTION COUNT",
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

        fig_map_1=px.choropleth(agg_trans_group,geojson=data1,locations="State",featureidkey="properties.ST_NM",
                                color="Transaction_amount",color_continuous_scale="ylgnbu",
                                range_color=(agg_tran_df["Transaction_amount"].min(),agg_tran_df["Transaction_amount"].max()),
                                hover_name ="State",title=  f"{agg_tran_df['Year'].min()} years {quarter} QUARTER  TRANSACTION AMOUNT",fitbounds="locations",
                                height= 650,width = 600)
        fig_map_1.update_geos(visible = False)
        
        st.plotly_chart(fig_map_1)
        
    with col2:
        
        fig_map_2=px.choropleth(agg_trans_group,geojson=data1,locations="State",featureidkey="properties.ST_NM",
                            color="Transaction_count",color_continuous_scale="ylorrd",
                            range_color=(agg_tran_df["Transaction_count"].min(),agg_tran_df["Transaction_count"].max()),
                            hover_name ="State",title= f"{agg_tran_df['Year'].min()} years {quarter} QUARTER  TRANSACTION COUNT",fitbounds="locations",
                            height= 650,width = 600)
        fig_map_2.update_geos(visible = False)

        st.plotly_chart(fig_map_2)
    
    return agg_tran_df
    

def map_amount_count_quarter(df,quarter):
    
    agg_tran_df=df[ df["Quarter"] == quarter]
    agg_tran_df.reset_index(drop=True, inplace=True)
    
    agg_trans_group = agg_tran_df.groupby('State')[['Transaction_amount','Transaction_count']].sum()
    agg_trans_group.reset_index(inplace=True)
    
    col1,col2 = st.columns(2)
    
    with col1:
    
        fig_amount= px.bar(agg_trans_group, x="State", y= "Transaction_amount",title= f"{agg_tran_df['Year'].min()} years {quarter} QUARTER TRANSACTION AMOUNT",
                                color_discrete_sequence=px.colors.sequential.Aggrnyl,height= 650,width = 600)
        
        st.plotly_chart(fig_amount)
    
    with col2:
    
        fig_count= px.bar(agg_trans_group, x="State", y= "Transaction_count",title=  f"{agg_tran_df['Year'].min()} years {quarter} QUARTER  TRANSACTION COUNT",
                                color_discrete_sequence=px.colors.sequential.Bluered_r,height= 650,width = 600)
        
        st.plotly_chart(fig_count)




def transaction_amount_type(df,states):
    
    agg_tran_df=df[ df["State"] == states]
    agg_tran_df.reset_index(drop=True, inplace=True)
    
    agg_trans_group = agg_tran_df.groupby('Transaction_type')[['Transaction_amount','Transaction_count']].sum()
    agg_trans_group.reset_index(inplace=True)
    
    col1,col2 = st.columns(2)
    
    with col1:
    
        fig_pie_1= px.pie (agg_trans_group,values = "Transaction_amount",names = "Transaction_type",
                            title = f"{states.upper()} TRANSACTION AMOUNT",hole = 0.5)
        
        st.plotly_chart(fig_pie_1)
    
    with col2:
    
        fig_pie_2= px.pie (agg_trans_group,values = "Transaction_count",names = "Transaction_type",
                            title = f"{states.upper()} TRANSACTION COUNT",hole = 0.5)
        
        st.plotly_chart(fig_pie_2)
        
        
    
def agg_user_type(df,years):
    agg_user_year=df[df["Year"] == years]
    agg_user_year.reset_index(drop=True, inplace=True)
    

    agg_user_year_group = agg_user_year.groupby('Transaction_brand')[['Transaction_count']].sum()
    agg_user_year_group.reset_index(inplace=True)
     

    fig_bar_1 = px.bar(agg_user_year_group, x="Transaction_brand", y= "Transaction_count",title= f"{years} BRANDS AND TRANSACTION COUNT",
                    width= 1000,color_discrete_sequence=px.colors.sequential.haline,hover_name= "Transaction_brand",height= 500)
    st.plotly_chart(fig_bar_1)
    
    return agg_user_year


# agg _user year quater

def agg_user_year_quater(df,quarter):
    agg_user_quater= df[df["Quarter"] == quarter]
    agg_user_quater.reset_index(drop=True, inplace=True)

    agg_user_quater_group = agg_user_quater.groupby('Transaction_brand')[['Transaction_count']].sum()
    agg_user_quater_group.reset_index(inplace=True)

    fig_bar_2 = px.bar(agg_user_quater_group, x="Transaction_brand", y= "Transaction_count",title=f"{quarter} QUARTER BRANDS AND TRANSACTION COUNT",
                    width= 1000,color_discrete_sequence=px.colors.sequential.Plasma_r,hover_name= "Transaction_brand",height= 500)
    st.plotly_chart(fig_bar_2)
    
    return agg_user_quater


# agg _user year quater

def agg_user2(df,states):

    aggr_quater_state =df[df["State"] == states]
    aggr_quater_state.reset_index(drop=True, inplace=True)

    fig_line_1 = px.line(aggr_quater_state, x="Transaction_brand", y="Transaction_count",hover_data="Transaction_percentage",
                        title= f"{states.upper()}  BRANDS-TRANSACTION COUNT-PERCENTAGE",width= 1000,markers = True)
    st.plotly_chart(fig_line_1)    



def map_trans_district_1(df,states):
    
    map_trans_df=df[ df["State"] == states]
    map_trans_df.reset_index(drop=True, inplace=True)
    
    map_trans_group = map_trans_df.groupby('Transaction_district')[['Transaction_amount','Transaction_count']].sum()
    map_trans_group.reset_index(inplace=True)
    
    col1,col2 = st.columns(2)
    
    with col1:
    
        fig_amount= px.bar(map_trans_group,x = "Transaction_amount",y= "Transaction_district",orientation = "h",height=800,
                            title = f"{states.upper()} DISTRICT-WISE TRANSACTION AMOUNT",
                            color_discrete_sequence=px.colors.sequential.Bluered_r)
        
        st.plotly_chart(fig_amount) 
            
    with col2:
    
        fig_count= px.bar (map_trans_group,x = "Transaction_count",y = "Transaction_district",orientation = "h",height=800,
                            title = f"{states.upper()} DISTRICT-WISE TRANSACTION COUNT",
                            color_discrete_sequence=px.colors.sequential.haline)
        
        st.plotly_chart(fig_count)










