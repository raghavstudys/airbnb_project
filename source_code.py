import streamlit as st
import pandas as pd
import psycopg2
import concurrent.futures
import plotly.graph_objects as go
import folium
from folium.plugins import MarkerCluster, AntPath
from streamlit_folium import folium_static
from psycopg2 import pool

st.set_page_config(
    page_icon='🏘️',
    layout="wide",
)

# Database connection pooling
connection_pool = pool.SimpleConnectionPool(1, 20,
                                            host='localhost',
                                            user='postgres',
                                            port="5432",
                                            database='airbnb',
                                            password="Shashi@007")

# Function to fetch data with pagination
def fetch_data(query, limit=1000, offset=0):
    conn = connection_pool.getconn()
    try:
        with conn.cursor() as curr:
            curr.execute(query, (limit, offset))
            data = curr.fetchall()
            columns = [desc[0] for desc in curr.description]
    finally:
        connection_pool.putconn(conn)
    return pd.DataFrame(data, columns=columns)

# Main data query with pagination
def main_d(limit=1000, offset=0):
    query2 = """
    WITH cte AS (
        SELECT _id, name, description, host_id, host_name, price, picture_url, host_is_superhost, host_location,
               LEFT(date::text, 10) AS date, weekly_price, monthly_price, amenities, 
               security_deposit, country, review_scores_accuracy, latitude, longitude
        FROM main_data 
        WHERE country IN ('China','Turkey','Spain','Brazil','Australia','Hong Kong','Canada',
                          'Portugal','United States')
    )
    SELECT * FROM cte
    LIMIT %s OFFSET %s
    """
    return fetch_data(query2, limit, offset)

def search():
    query = "SELECT DISTINCT country FROM main_data"
    return fetch_data(query, limit=1000, offset=0)

def map_view(main_data):
    if 'latitude' not in main_data.columns or 'longitude' not in main_data.columns:
        st.error("Latitude and Longitude columns are missing in the data.")
        return
    
    map_center = [main_data['latitude'].median(), main_data['longitude'].median()]
    mymap = folium.Map(
        location=map_center,
        zoom_start=5,
        tiles='OpenStreetMap'
    )
    marker_cluster = MarkerCluster().add_to(mymap)
    for index, row in main_data.iterrows():
        popup_html = f"""
            <b>{row['name']}</b><br>
            Price: {row['price']}<br>
            Review Score: {row['review_scores_accuracy']} ⭐️<br>
            Country: {row['country']}<br>
            Monthly Price: {row['monthly_price']}<br>
            <a href="{row['picture_url']}" target="_blank">View Hotel</a>
        """
        folium.Marker(
            location=[row['latitude'], row['longitude']],
            popup=folium.Popup(popup_html, max_width=400),
            icon=folium.Icon(color='red', icon='info-sign')
        ).add_to(marker_cluster)
    locations = list(zip(main_data['latitude'], main_data['longitude']))
    AntPath(locations, delay=100, color='violet', weight=1.8, pulse_color='white').add_to(mymap)
    st.header(":black[Airbnb Listings]")
    folium_static(mymap, width=1000, height=500)

# Fetch initial data concurrently
with concurrent.futures.ThreadPoolExecutor() as executor:
    future_search_country = executor.submit(search)
    future_main_data = executor.submit(main_d, limit=500, offset=0)

search_country = future_search_country.result()
main_data = future_main_data.result()

col1, col2 = st.columns([13, 69])
with col1:
    st.image('/Users/shanthakumark/Downloads/Airbnb-Logo-PNG-Photos.webp')

with col2:
    st.write("")
    tab1, tab2, tab3 = st.tabs(["Where - Search Destination", "Locate Us📍","Deep-Dive 🤿"])

    with tab1:
        location_selected = st.selectbox("Select the Location", search_country['country'].to_list())
        present_currency = {
            "Canada": "CAD",
            "Turkey": "TRY",
            "Spain": "EUR",
            "Brazil": "BRL",
            "Australia": "AUD",
            "Hong Kong": "HKD",
            "United States": "USD"
        }.get(location_selected, "USD")

        budget = st.checkbox(":green[Budget]")
        highclass = st.checkbox(":red[High Class]")

        selected_data = main_data[main_data['country'] == location_selected]
        final_s_data_1 = selected_data.sort_values(by='review_scores_accuracy', ascending=False).reset_index(drop=True)

        if budget:
            avg_f = final_s_data_1['price'].mean()
            final_s_data = final_s_data_1[final_s_data_1['price'] < avg_f]
        elif highclass:
            avg_h = final_s_data_1['price'].mean()
            final_s_data = final_s_data_1[final_s_data_1['price'] > avg_h]
        else:
            final_s_data = final_s_data_1

        slide = st.slider(min_value=0, max_value=max(final_s_data['price']), label="Price Range")
        if slide:
            final_s_data = final_s_data[final_s_data['price'] < slide]

        st.write(f":black[Total Hotel available on this Location:] :red[{len(final_s_data['name'])}]")

        for i in range(len(final_s_data)):
            cols2_1, cols2_2 = st.columns([10, 3])
            with cols2_1:
                hotel_name = final_s_data["name"].iloc[i]
                f_h_n = hotel_name.title()
                st.subheader(f'{i+1}) {f_h_n}', divider="rainbow")
            with cols2_2:
                s_h = "🦸 I'm a superhost" if final_s_data["host_is_superhost"].iloc[i] == "true" else ""
                st.markdown(f"<h3 style='color: gold;'>{final_s_data['review_scores_accuracy'].iloc[i]} ⭐️</h3>", unsafe_allow_html=True)
                st.write(s_h)

            cols1, cols2 = st.columns([1, 4])
            with cols1:
                st.markdown(f"<h3 style='color: green;'>Price : {final_s_data['price'].iloc[i]} {present_currency}</h3>", unsafe_allow_html=True)
                r_picture = final_s_data['picture_url'].iloc[i]
                st.image(r_picture, width=210)
                st.area_chart(data=final_s_data, x='name', y='price', width=230, height=230, color='#c6072d')

                if 'date' in final_s_data.columns:
                    date_entered = final_s_data['date'].iloc[i]
                    st.markdown(f"At Service From: {date_entered}")

            with cols2:
                st.markdown(f"<h3 style='color: black;'> Hotel ID: {final_s_data['_id'].iloc[i]} 🏨 , Host Name: {final_s_data['host_name'].iloc[i]} 🧍🏻 , Host id: {final_s_data['host_id'].iloc[i]}</h3>", unsafe_allow_html=True)
                st.markdown(f"""
                    <div style="display: flex; justify-content: space-between;">
                        <div style="border: 2px solid; border-image: linear-gradient(to right); padding: 10px; margin-right: 15px; text-align: center;">
                            <h4>Security Deposit</h4>
                            <p style="color: green; margin: 0;">{final_s_data['security_deposit'].iloc[i]} {present_currency}</p>
                        </div>
                        <div style="border: 2px solid; border-image: linear-gradient(to right); padding: 10px; text-align: center;">
                            <h4>Weekly Price & Monthly Price</h4>
                            <p style="color: green; margin: 0;">for a Week: {final_s_data['weekly_price'].iloc[i]} {present_currency} / for a Month {final_s_data['monthly_price'].iloc[i]} {present_currency}</p>
                        </div>
                    </div>
                """, unsafe_allow_html=True)
                st.write("---")
                data_expander = st.expander("Amenities 🖥️", expanded=False)
                with data_expander:
                    amenities_ = final_s_data['amenities'].iloc[i]
                    F_a = amenities_.replace("[", '').replace("]", "").replace("'", "")
                    st.markdown(f":violet[{F_a}]")
                data_expander_2 = st.expander("Description Of Hotel 🏨", expanded=False)
                with data_expander_2:
                    Description_ = final_s_data['description'].iloc[i]
                    st.markdown(f":orange[{Description_}]")

    with tab2:
        map_view(main_data)
        our_location = st.expander("Our Location's")
        with our_location:
            st.title("Our Location")
            our = main_data[['country']].sort_values(by='country', ascending=True)
            for i in list(our['country'].unique()):
                st.write(f'🔸 {i}')

        our_host = st.expander("Our Host's From")
        with our_host:
            st.title("Our Hosts From")
            host_l = main_data[['host_location']].sort_values(by='host_location', ascending=True)
            for j in list(host_l['host_location'].unique()):
                st.write(f'☀︎ {j}')
    with tab3:
        pivoted_views = pd.pivot_table(main_data,index='country',values='price',aggfunc='mean').reset_index()
        con = st.container(height=430,border=True)
        with con:
            st.header('Country Wise Price',divider='rainbow',anchor= 'Country Wise Price')
            st.area_chart(pivoted_views,x = 'country',y = 'price',use_container_width= True,color='#FF3396')
        comments = st.expander("See what our customers say about us")
        with comments:
            connection_pool = pool.SimpleConnectionPool(1, 20,
                                            host='localhost',
                                            user='postgres',
                                            port="5432",
                                            database='airbnb',
                                            password="Shashi@007")   
            query_c = """select main_data.name as h_name,main_data.country as country, reviewer_data.reviewer_name,left(reviewer_data.date::text,10) as r_date,reviewer_data.review_scores_rating,reviewer_data.comments from reviewer_data left join main_data on reviewer_data._id = main_data._id LIMIT 25"""
            curr = connection_pool.getconn()
            with curr.cursor() as curr:
                curr.execute(query_c)
                data = curr.fetchall()
                columns = [desc[0] for desc in curr.description]
                comme = pd.DataFrame(data,columns=columns)
            for i in range(len(comme)):
                colc2_1, colc2_2 = st.columns([10, 3])
                with colc2_1:
                    name = comme["reviewer_name"].iloc[i]
                    f_c_n = name.title()
                    st.subheader(f'{i+1}) {f_c_n}', divider="rainbow")
                    d = comme['r_date'].iloc[i]
                    h_n = comme['h_name'].iloc[i]
                    c_n = comme['country'].iloc[i]
                    st.write(f'Posted on: {d}')
                    b_1 = st.container(height= 40)
                    with b_1:
                        st.write(f'{h_n}, {c_n}')
                with colc2_2:
                    s_h = comme["comments"].iloc[i]
                    st.markdown(f"<h3 style='color: gold;'>{comme['review_scores_rating'].iloc[i]} ⭐️</h3>", unsafe_allow_html=True)
                    comcon = st.container(height = 200,border= True)
                    with comcon:
                        st.write(s_h)
        co3 = st.container(height = 400)
        with co3:
            st.video('https://youtu.be/QChu4yeAxhU?si=S6GVBZzAV2ghCyzj',start_time=5,loop=True,autoplay=True,muted = True)       

