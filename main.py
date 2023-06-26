import mysql.connector
import os
import pandas as pd
import json
import sqlalchemy
from sqlalchemy import create_engine
import streamlit as st
import requests
import plotly.express as px

path="/Users/xaviersavarimuthu/PycharmProjects/phone.pe_pulse/pulse/data/aggregated/transaction/country/india/state/"
Agg_state_list=os.listdir(path)
clm={'State': [], 'Year': [], 'Quarter': [], 'Transaction_type': [], 'Transaction_count': [], 'Transaction_amount': []}
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
              clm['Transaction_type'].append(Name)
              clm['Transaction_count'].append(count)
              clm['Transaction_amount'].append(amount)
              clm['State'].append(i)
              clm['Year'].append(j)
              clm['Quarter'].append(int(k.strip('.json')))

df_aggregated_transaction=pd.DataFrame(clm)

path2="/Users/xaviersavarimuthu/PycharmProjects/phone.pe_pulse/pulse/data/aggregated/user/country/india/state/"
user_list = os.listdir(path2)
col2 = {'State': [], 'Year': [], 'Quarter': [], 'Brands': [], 'User_Count': [], 'User_Percentage': []}
for i in user_list:
    p_i = path2 + i + "/"
    Agg_yr = os.listdir(p_i)

    for j in Agg_yr:
        p_j = p_i + j + "/"
        Agg_yr_list = os.listdir(p_j)

        for k in Agg_yr_list:
            p_k = p_j + k

            Data = open(p_k, 'r')
            B = json.load(Data)
            try:
                for w in B["data"]["usersByDevice"]:
                    brand_name = w["brand"]
                    count_ = w["count"]
                    ALL_percentage = w["percentage"]
                    col2["Brands"].append(brand_name)
                    col2['User_Count'].append(count_)
                    col2['User_Percentage'].append(ALL_percentage)
                    col2["State"].append(i)
                    col2["Year"].append(j)
                    col2["Quarter"].append(int(k.strip('.json')))
            except:
                pass
df_aggregated_user = pd.DataFrame(col2)

path3="/Users/xaviersavarimuthu/PycharmProjects/phone.pe_pulse/pulse/data/map/transaction/hover/country/india/state/"
hover_list = os.listdir(path3)
col3 = {'State': [], 'Year': [], 'Quarter': [], 'District': [], 'Transaction_Count': [], 'Transaction_Amount': []}
for i in hover_list:
    p_i = path3 + i + "/"
    Agg_yr = os.listdir(p_i)

    for j in Agg_yr:
        p_j = p_i + j + "/"
        Agg_yr_list = os.listdir(p_j)

        for k in Agg_yr_list:
            p_k = p_j + k

            Data = open(p_k, 'r')
            C = json.load(Data)
            for x in C["data"]["hoverDataList"]:
                District = x["name"]
                count = x["metric"][0]["count"]
                amount = x["metric"][0]["amount"]
                col3["District"].append(District)
                col3['Transaction_Count'].append(count)
                col3['Transaction_Amount'].append(amount)
                col3['State'].append(i)
                col3['Year'].append(j)
                col3['Quarter'].append(int(k.strip('.json')))
df_map_transaction = pd.DataFrame(col3)
path4="/Users/xaviersavarimuthu/PycharmProjects/phone.pe_pulse/pulse/data/map/user/hover/country/india/state/"
map_list = os.listdir(path4)

col4 = {"State": [], "Year": [], "Quarter": [], "District": [],
        "Registered_User": []}
for i in map_list:

    p_i = path4 + i + "/"
    Agg_yr = os.listdir(p_i)

    for j in Agg_yr:
        p_j = p_i + j + "/"
        Agg_yr_list = os.listdir(p_j)

        for k in Agg_yr_list:
            p_k = p_j + k

            Data = open(p_k, 'r')
            D = json.load(Data)

            for u in D["data"]["hoverData"].items():

                district = u[0]
                registereduser = u[1]["registeredUsers"]
                col4["District"].append(district)
                col4["Registered_User"].append(registereduser)
                col4['State'].append(i)
                col4['Year'].append(j)
                col4['Quarter'].append(int(k.strip('.json')))
df_map_user = pd.DataFrame(col4)
path5="/Users/xaviersavarimuthu/PycharmProjects/phone.pe_pulse/pulse/data/top/transaction/country/india/state/"
TOP_list = os.listdir(path5)

col5 = {'State': [], 'Year': [], 'Quarter': [], 'District_Pincode': [], 'Transaction_count': [], 'Transaction_amount': []}
for i in TOP_list:
    p_i = path5 + i + "/"

    Agg_yr = os.listdir(p_i)

    for j in Agg_yr:
        p_j = p_i + j + "/"
        Agg_yr_list = os.listdir(p_j)

        for k in Agg_yr_list:
            p_k = p_j + k

            Data = open(p_k, 'r')
            E = json.load(Data)
            for z in E['data']['pincodes']:
                Name = z['entityName']
                count = z['metric']['count']
                amount = z['metric']['amount']
                col5['District_Pincode'].append(Name)
                col5['Transaction_count'].append(count)
                col5['Transaction_amount'].append(amount)
                col5['State'].append(i)
                col5['Year'].append(j)
                col5['Quarter'].append(int(k.strip('.json')))
df_top_transaction = pd.DataFrame(col5)
path6="/Users/xaviersavarimuthu/PycharmProjects/phone.pe_pulse/pulse/data/top/user/country/india/state/"
USER_list = os.listdir(path6)

col6 = {'State': [], 'Year': [], 'Quarter': [], 'District_Pincode': [],
        'Registered_User': []}
for i in USER_list:
    p_i = path6 + i + "/"
    Agg_yr = os.listdir(p_i)

    for j in Agg_yr:
        p_j = p_i + j + "/"
        Agg_yr_list = os.listdir(p_j)

        for k in Agg_yr_list:
            p_k = p_j + k

            Data = open(p_k, 'r')
            F = json.load(Data)
            for t in F['data']['pincodes']:
                Name = t['name']
                registeredUser = t['registeredUsers']
                col6['District_Pincode'].append(Name)
                col6['Registered_User'].append(registeredUser)
                col6['State'].append(i)
                col6['Year'].append(j)
                col6['Quarter'].append(int(k.strip('.json')))
df_top_user = pd.DataFrame(col6)

db = mysql.connector.connect(
    host='localhost',
    user='root',
    password='12345678',
    database='phone.pe_1'
)

mycursor = db.cursor()

engine = create_engine('mysql+mysqlconnector://root:12345678@localhost/phone.pe_1', echo=False)

df_aggregated_transaction.to_sql('aggregated_transaction', engine, if_exists='replace',
                                dtype={'State': sqlalchemy.types.VARCHAR(length=50),
                                       'Year': sqlalchemy.types.Integer,
                                       'Quarter': sqlalchemy.types.Integer,
                                       'Transaction_type': sqlalchemy.types.VARCHAR(length=50),
                                       'Transaction_count': sqlalchemy.types.Integer,
                                       'Transaction_amount': sqlalchemy.types.FLOAT(precision=5, asdecimal=True)})


df_aggregated_user.to_sql('aggregated_user', engine, if_exists='replace',
                         dtype={'State': sqlalchemy.types.VARCHAR(length=50),
                                 'Year': sqlalchemy.types.Integer,
                                 'Quarter': sqlalchemy.types.Integer,
                                 'Brands': sqlalchemy.types.VARCHAR(length=50),
                                 'User_Count': sqlalchemy.types.Integer,
                                 'User_Percentage': sqlalchemy.types.FLOAT(precision=5, asdecimal=True)})


df_map_transaction.to_sql('map_transaction', engine, if_exists='replace',
                          dtype={'State': sqlalchemy.types.VARCHAR(length=50),
                                 'Year': sqlalchemy.types.Integer,
                                 'Quarter': sqlalchemy.types.Integer,
                                 'District': sqlalchemy.types.VARCHAR(length=50),
                                 'Transaction_Count': sqlalchemy.types.Integer,
                                 'Transaction_Amount': sqlalchemy.types.FLOAT(precision=5, asdecimal=True)})

df_map_user.to_sql('map_user', engine, if_exists='replace',
                  dtype={'State': sqlalchemy.types.VARCHAR(length=50),
                          'Year': sqlalchemy.types.Integer,
                          'Quarter': sqlalchemy.types.Integer,
                          'District': sqlalchemy.types.VARCHAR(length=50),
                          'Registered_User': sqlalchemy.types.Integer, })

df_top_transaction.to_sql('top_transaction', engine, if_exists='replace',
                         dtype={'State': sqlalchemy.types.VARCHAR(length=50),
                                'Year': sqlalchemy.types.Integer,
                                'Quarter': sqlalchemy.types.Integer,
                                'District_Pincode': sqlalchemy.types.Integer,
                                'Transaction_count': sqlalchemy.types.Integer,
                                'Transaction_amount': sqlalchemy.types.FLOAT(precision=5, asdecimal=True)})

df_top_user.to_sql('top_user', engine, if_exists = 'replace', index=False,
                   dtype={'State': sqlalchemy.types.VARCHAR(length=50),
                          'Year': sqlalchemy.types.Integer,
                          'Quarter': sqlalchemy.types.Integer,
                          'District_Pincode': sqlalchemy.types.Integer,
                          'Registered_User': sqlalchemy.types.Integer})




st.title(':violet[ PhonePe Pulse Data Visualization ]')
option = st.radio('**Select your option**',('All India', 'State wise','Top Ten categories'),horizontal=True)

if option == 'All India':
    tab1, tab2 = st.tabs(['Transaction','User'])

    with tab1:
        col1, col2, col3 = st.columns(3)
        with col1:
            trans_year = st.selectbox('**Select Year**', ('2018','2019','2020','2021','2022'),key='trans_year')
        with col2:
            trans_quarter= st.selectbox('**Select Quarter**', ('1','2','3','4'),key='trans_quarter')
        with col3:
            trans_type = st.selectbox('**Select Transaction type**', ('Recharge & bill payments','Peer-to-peer payments',
            'Merchant payments','Financial Services','Others'),key='trans_type')
        mycursor.execute(
            f"SELECT State, Transaction_amount FROM aggregated_transaction WHERE Year = '{trans_year}' AND Quarter = '{trans_quarter}' AND Transaction_type = '{trans_type}';")
        trans_bar_result=pd.DataFrame(mycursor.fetchall(),columns=['State', 'Transaction_amount'])
        trans_bar_result_1 = trans_bar_result.set_index(pd.Index(range(1, len(trans_bar_result) + 1)))

        mycursor.execute(
            f"SELECT State, Transaction_count, Transaction_amount FROM aggregated_transaction WHERE Year = '{trans_year}' AND Quarter = '{trans_quarter}' AND Transaction_type = '{trans_type}';")
        trans_tab_result=pd.DataFrame(mycursor.fetchall(),columns=['State','Transaction_count','Transaction_amount'])



        mycursor.execute(
            f"SELECT SUM(Transaction_amount), AVG(Transaction_amount) FROM aggregated_transaction WHERE Year = '{trans_year}' AND Quarter = '{trans_quarter}' AND Transaction_type = '{trans_type}';")

        trans_total_amount=pd.DataFrame(mycursor.fetchall(),columns=['Total','Average'])
        mycursor.execute(
            f"SELECT SUM(Transaction_count), AVG(Transaction_count) FROM aggregated_transaction WHERE Year = '{trans_year}' AND Quarter = '{trans_quarter}' AND Transaction_type = '{trans_type}';")
        trans_total_count=pd.DataFrame(mycursor.fetchall(),columns=['Total','Average'])

        # geo_plot
        trans_bar_result.drop(columns=['State'],inplace=True)

        url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
        response = requests.get(url)
        data1 = json.loads(response.content)
        state_names_tra = [i['properties']['ST_NM'] for i in data1['features']]
        state_names_tra.sort()
        df_state_names_tra = pd.DataFrame({'State': state_names_tra})
        df_state_names_tra['Transaction_amount'] = trans_bar_result
        df_state_names_tra.to_csv('State_trans.csv', index=False)
        # Read csv
        df_tra = pd.read_csv('State_trans.csv')
        # Geo plot


        fig_tra = px.choropleth(
            df_tra,
            geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
            featureidkey='properties.ST_NM', locations='State', color='Transaction_amount',
            color_continuous_scale='thermal',title='Transaction Analysis')
        fig_tra.update_geos(fitbounds="locations", visible=False)
        fig_tra.update_layout(title_font=dict(size=33), title_font_color='#6739b7', height=800)
        st.plotly_chart(fig_tra, use_container_width=True)


        trans_bar_result_1['State'] = trans_bar_result_1['State'].astype(str)
        trans_bar_result_1['Transaction_amount'] = trans_bar_result_1['Transaction_amount'].astype(float)
        trans_bar_chart = px.bar(trans_bar_result_1, x='State', y='Transaction_amount',
                                            color='Transaction_amount', color_continuous_scale='thermal',
                                            title='Transaction Analysis Chart', height=700, )
        trans_bar_chart.update_layout(title_font=dict(size=33), title_font_color='#6739b7')
        st.plotly_chart(trans_bar_chart, use_container_width=True)

        st.header(':violet[Total calculation]')

        col4, col5 = st.columns(2)
        with col4:
            st.subheader('Transaction Analysis')
            st.dataframe(trans_tab_result)
        with col5:
            st.subheader('Transaction Amount')
            st.dataframe(trans_total_amount)
            st.subheader('Transaction Count')
            st.dataframe(trans_total_count)
    with tab2:
        col1, col2 = st.columns(2)
        with col1:
            user_year = st.selectbox('**Select Year**', ('2018', '2019', '2020', '2021', '2022'), key='user_year')
        with col2:
            user_quarter = st.selectbox('**Select Quarter**', ('1', '2', '3', '4'), key='user_quarter')

        mycursor.execute(
            f"SELECT State, SUM(User_Count) FROM aggregated_user WHERE Year = '{user_year}' AND Quarter = '{user_quarter}' GROUP BY State")
        user_bar_result=pd.DataFrame(mycursor.fetchall(),columns=['State','User_count'])
        user_bar_result_1 = user_bar_result.set_index(pd.Index(range(1, len(user_bar_result) + 1)))

        mycursor.execute(
            f"SELECT SUM(User_Count), AVG(User_Count) FROM aggregated_user WHERE Year = '{user_year}' AND Quarter = '{user_quarter}'")
        user_tab_result=pd.DataFrame(mycursor.fetchall(),columns=['Total_User_Count','Avg_User_count'])

        user_bar_result.drop(columns=['State'], inplace=True)
        url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
        response = requests.get(url)
        data2 = json.loads(response.content)
        # Extract state names and sort them in alphabetical order
        state_names_use = [i['properties']['ST_NM'] for i in data2['features']]
        state_names_use.sort()
        # Create a DataFrame with the state names column
        df_state_names_use = pd.DataFrame({'State': state_names_use})
        # Combine the Gio State name with user_bar_result
        df_state_names_use['User Count'] = user_bar_result
        # convert dataframe to csv file
        df_state_names_use.to_csv('State_user.csv', index=False)
        # Read csv
        df_use = pd.read_csv('State_user.csv')
        # Geo plot
        fig_use = px.choropleth(
            df_use,
            geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
            featureidkey='properties.ST_NM', locations='State', color='User Count', color_continuous_scale='thermal',
            title='User Analysis')
        fig_use.update_geos(fitbounds="locations", visible=False)
        fig_use.update_layout(title_font=dict(size=33), title_font_color='#6739b7', height=800)
        st.plotly_chart(fig_use, use_container_width=True)


        user_bar_result_1['State'] = user_bar_result_1['State'].astype(str)
        user_bar_result_1['User_count'] = user_bar_result_1['User_count'].astype(float)
        user_bar_chart = px.bar(user_bar_result_1, x='State', y='User_count',
                                 color='User_count', color_continuous_scale='thermal',
                                 title='User Analysis Chart', height=700, )
        user_bar_chart.update_layout(title_font=dict(size=33), title_font_color='#6739b7')
        st.plotly_chart(user_bar_chart, use_container_width=True)

        st.header(':violet[Total calculation]')

        col3, col4 = st.columns(2)
        with col3:
            st.subheader('User Analysis')
            st.dataframe(user_bar_result_1)
        with col4:
            st.subheader('User Count')
            st.dataframe(user_tab_result)

elif option == 'State wise':
    tab3, tab4 = st.tabs(['Transaction', 'User'])

    with tab3:
        col1, col2, col3 = st.columns(3)
        with col1:
            states_select = st.selectbox('**Select State**', (
                'andaman-&-nicobar-islands', 'andhra-pradesh', 'arunachal-pradesh', 'assam', 'bihar',
                'chandigarh', 'chhattisgarh', 'dadra-&-nagar-haveli-&-daman-&-diu', 'delhi', 'goa', 'gujarat',
                'haryana', 'himachal-pradesh',
                'jammu-&-kashmir', 'jharkhand', 'karnataka', 'kerala', 'ladakh', 'lakshadweep', 'madhya-pradesh',
                'maharashtra', 'manipur',
                'meghalaya', 'mizoram', 'nagaland', 'odisha', 'puducherry', 'punjab', 'rajasthan', 'sikkim',
                'tamil-nadu', 'telangana',
                'tripura', 'uttar-pradesh', 'uttarakhand', 'west-bengal'), key='states_select')
        with col2:
            st_se_year = st.selectbox('**Select Year**', ('2018', '2019', '2020', '2021', '2022'), key='st_se_year')
        with col3:
            st_se_quarter = st.selectbox('**Select Quarter**', ('1', '2', '3', '4'), key='st_se_quarter')

        mycursor.execute(
            f"SELECT Transaction_type, Transaction_amount FROM aggregated_transaction WHERE State = '{states_select}' AND Year = '{st_se_year}' AND Quarter = '{st_se_quarter}';")
        st_an_bar_res = pd.DataFrame(mycursor.fetchall(), columns=['Transaction_type', 'Transaction_amount'])

        mycursor.execute(
            f"SELECT Transaction_type, Transaction_count, Transaction_amount FROM aggregated_transaction WHERE State = '{states_select}' AND Year = '{st_se_year}' AND Quarter = '{st_se_quarter}';")
        st_an_tab_res = pd.DataFrame(mycursor.fetchall(),
                                     columns=['Transaction_type', 'Transaction_count', 'Transaction_amount'])

        mycursor.execute(
            f"SELECT SUM(Transaction_amount), AVG(Transaction_amount) FROM aggregated_transaction WHERE State = '{states_select}' AND Year = '{st_se_year}' AND Quarter = '{st_se_quarter}';")
        st_an_to_amount_tab = pd.DataFrame(mycursor.fetchall(), columns=['Total', 'Average'])

        mycursor.execute(
            f"SELECT SUM(Transaction_count), AVG(Transaction_count) FROM aggregated_transaction WHERE State = '{states_select}' AND Year ='{st_se_year}' AND Quarter = '{st_se_quarter}';")
        st_an_to_count_tab = pd.DataFrame(mycursor.fetchall(), columns=['Total', 'Average'])

        st_an_trans_fig = px.bar(st_an_bar_res, x='Transaction_type', y='Transaction_amount',
                                 color='Transaction_amount',
                                 color_continuous_scale='thermal', title='Transaction Analysis Chart', height=500, )
        st_an_trans_fig.update_layout(title_font=dict(size=33), title_font_color='#6739b7')
        st.plotly_chart(st_an_trans_fig, use_container_width=True)

        st.header(':violet[Total calculation]')

        col4, col5 = st.columns(2)
        with col4:
            st.subheader('Transaction Analysis')
            st.dataframe(st_an_tab_res)
        with col5:
            st.subheader('Transaction Amount')
            st.dataframe(st_an_to_amount_tab)
            st.subheader('Transaction Count')
            st.dataframe(st_an_to_count_tab)

    with tab4:


        col5, col6 = st.columns(2)
        with col5:
            states_select_user = st.selectbox('**Select State**', (
                'andaman-&-nicobar-islands', 'andhra-pradesh', 'arunachal-pradesh', 'assam', 'bihar',
                'chandigarh', 'chhattisgarh', 'dadra-&-nagar-haveli-&-daman-&-diu', 'delhi', 'goa', 'gujarat',
                'haryana', 'himachal-pradesh',
                'jammu-&-kashmir', 'jharkhand', 'karnataka', 'kerala', 'ladakh', 'lakshadweep', 'madhya-pradesh',
                'maharashtra', 'manipur',
                'meghalaya', 'mizoram', 'nagaland', 'odisha', 'puducherry', 'punjab', 'rajasthan', 'sikkim',
                'tamil-nadu', 'telangana',
                'tripura', 'uttar-pradesh', 'uttarakhand', 'west-bengal'), key='states_select_user')
        with col6:
            state_select_year = st.selectbox('**Select Year**', ('2018', '2019', '2020', '2021', '2022'),
                                             key='state_select_year')
        mycursor.execute(
            f"SELECT Quarter, SUM(User_Count) FROM aggregated_user WHERE State = '{states_select_user}' AND Year = '{state_select_year}' GROUP BY Quarter;")
        states_user_bar_res = pd.DataFrame(mycursor.fetchall(), columns=['Quarter', 'User_Count'])

        mycursor.execute(
            f"SELECT SUM(User_Count), AVG(User_Count) FROM aggregated_user WHERE State = '{states_select_user}' AND Year = '{state_select_year}';")
        states_total_user_tab_result = pd.DataFrame(mycursor.fetchall(), columns=['Total', 'Average'])

        # states_user_bar_res['Quarter'].astype('int')
        # states_user_bar_res['User_Count'].astype('int')

        user_bar_fig = px.bar(states_user_bar_res, x='Quarter', y='User_Count', color='User_Count',
                              color_continuous_scale='thermal', title='User Analysis Chart', height=500)
        user_bar_fig.update_layout(title_font=dict(size=33), title_font_color='#6739b7')
        st.plotly_chart(user_bar_fig, use_container_width=True)

        st.header(':violet[Total calculation]')
        st.subheader('User Analysis')
        st.dataframe(states_total_user_tab_result)

else:
    tab5, tab6 = st.tabs(['Transaction', 'User'])
    with tab5:
        top_trans_year = st.selectbox('**Select Year**', ('2018', '2019', '2020', '2021', '2022'), key='top_trans_year')

        mycursor.execute(
            f"SELECT State, SUM(Transaction_amount) As Transaction_amount FROM top_transaction WHERE Year = '{top_trans_year}' GROUP BY State ORDER BY Transaction_amount DESC LIMIT 10;")
        top_trans_bar_res=pd.DataFrame(mycursor.fetchall(), columns=['State', 'Top_Transaction_amount'])

        mycursor.execute(
            f"SELECT State, SUM(Transaction_amount) as Transaction_amount, SUM(Transaction_count) as Transaction_count FROM top_transaction WHERE Year = '{top_trans_year}' GROUP BY State ORDER BY Transaction_amount DESC LIMIT 10;")

        top_trans_tab_res=pd.DataFrame(mycursor.fetchall(), columns=['State','Transaction_amount', 'Transaction_count'])

        # top_trans_bar_res['State']=top_trans_bar_res['State'].astype(str)
        # top_trans_bar_res['Top_Transaction_amount']=top_trans_bar_res['Top_Transaction_amount'].astype(float)

        top_trans_fig=px.bar(top_trans_bar_res, x='State',y='Top_Transaction_amount',color='Top_Transaction_amount',color_continuous_scale = 'thermal', title = 'Top Transaction Analysis Chart', height = 600)
        top_trans_fig.update_layout(title_font=dict(size=33), title_font_color='#6739b7')
        st.plotly_chart(top_trans_fig, use_container_width=True)
        st.header(':violet[Total calculation]')
        st.subheader('Top Transaction Analysis')
        st.dataframe(top_trans_tab_res)
    with tab6:
        top_user_year = st.selectbox('**Select Year**', ('2018', '2019', '2020', '2021', '2022'), key='top_user_year')

        mycursor.execute(
            f"SELECT State, SUM(Registered_User) AS Top_user FROM top_user WHERE Year='{top_user_year}' GROUP BY State ORDER BY Top_user DESC LIMIT 10;")
        top_user_bar_res=pd.DataFrame(mycursor.fetchall(), columns=['State','Top_User_Count'])

        top_user_fig=px.bar(top_user_bar_res,x='State',y='Top_User_Count',color='Top_User_Count',color_continuous_scale = 'thermal', title = 'Top User Analysis Chart', height = 600)
        top_user_fig.update_layout(title_font=dict(size=33),title_font_color='#6739b7')
        st.plotly_chart(top_user_fig,use_container_width=True)

        st.header(':violet[Total calculation]')
        st.subheader('Total User Analysis')
        st.dataframe(top_user_bar_res)







































