import requests
from bs4 import BeautifulSoup
import re
import math
import numpy as np
import pandas as pd
import geopandas as gpd
from datetime import datetime
from sqlalchemy import create_engine
import psycopg2
from pei_webscrape_functions import unique_id_check,active_listings_insert,active_listing_update,days_on_market_calc,days_on_market_update,spatial_update,lot_size_extract


session = requests.Session()

headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) '
                  'Chrome/104.0.5112.79 Safari/537.36'
}
url = 'https://www.zolo.ca/index.php?sarea=PE&s=1'
response = session.get(url, headers=headers)

soup = BeautifulSoup(response.content, "lxml")

pages = math.ceil((int(soup.find(class_= "drawer-header-value xs-line-height-60 xs-relative").find("span").contents[0]) / 36))

listing_links = []
listing_date = []
address_list = []
city_list = []
province_list = []
bedroom_list = []
bathroom_list = []
sqft_list = []
price_list = []
lat_list = []
long_list = []
broker_list = []
parking_list = []
mls_list = []
year_built_list = []
lot_size_list = []
property_type_list = []
attachement_list = []
ownership_list = []
basement_list = []

for i in range(1,(pages+1)):
    url1 = f"https://www.zolo.ca/index.php?sarea=PE&s={i}"
    response1 = session.get(url1, headers=headers)
    soup1 = BeautifulSoup(response1.content, "html.parser")
    for link in soup1.find_all(class_="card-listing--image fill-white xs-relative xs-overflow-hidden"):
        l = link.find(['a'], href = True)
        href = re.search('(?<=href=").*?(?=")', str(l)).group(0)
        listing_links.append(href)

for i in listing_links:
    listing_response = session.get(i, headers=headers)
    listing_soup = BeautifulSoup(listing_response.content, "html.parser")
    try:
        if listing_soup.find('dl', class_ = 'column key-fact-mls'):
            mls_list.append(str(listing_soup.find('dl', class_ = 'column key-fact-mls').find(class_ = 'priv').contents[0]))
        else:
            mls_list.append(None)
    except:
        mls_list.append(None)
    try:
        if listing_soup.find(['tbody'],class_ ='table-group').find:
            listing_date.append(str(listing_soup.find(['tbody'],class_ ='table-group').find(['td'], class_ = 'table-date xs-col-3 sm-col-4 nowrap').contents[0]))
        else:
            listing_date.append(None)
    except:
        listing_date.append(None)
    try:
        if listing_soup.find('dt', class_ = 'column-label', text = 'Listed By'):
            broker_list.append(str(listing_soup.find('dt', class_ = 'column-label', text = 'Listed By').find_next_sibling('dd').contents[0]))
        else:
            broker_list.append(None)
    except:
        broker_list.append(None)
    try:
        if listing_soup.find('dt', class_ = 'column-label', text = 'Type'):
            property_type_list.append(str(listing_soup.find('dt', class_ = 'column-label', text = 'Type').find_next_sibling('dd').find(class_ = 'priv').contents[0]))
        else:
            property_type_list.append(None)
    except:
        property_type_list.append(None)
    try:
        if listing_soup.find('div', class_ = 'column-label', text = 'Attachment'):
            attachement_list.append(str(listing_soup.find('div', class_ = 'column-label', text = 'Attachment').find_next_sibling('div').find(class_ = 'priv').contents[0]))
        else:
            attachement_list.append(None)
    except:
        attachement_list.append(None)
    try:
        if listing_soup.find('div', class_ = 'column-label', text = 'Year Built'):
            year_built_list.append(str(listing_soup.find('div', class_ = 'column-label', text = 'Year Built').find_next_sibling('div').find(class_ = 'priv').contents[0]))
        else:
            year_built_list.append(None)
    except:
        year_built_list.append(None)
    try:
        if listing_soup.find('div', class_ = 'column-label', text = 'Ownership'):
            ownership_list.append(str(listing_soup.find('div', class_ = 'column-label', text = 'Ownership').find_next_sibling('div').find(class_ = 'priv').contents[0]))
        else:
            ownership_list.append(None)
    except:
        ownership_list.append(None)
    try:
        if listing_soup.find('div', class_ = 'column-label', text = 'Bedrooms'):
            bedroom_list.append(int(listing_soup.find('div', class_ = 'column-label', text = 'Bedrooms').find_next_sibling('div').find(class_ = 'priv').contents[0]))
        else:
            bedroom_list.append(None)
    except:
        bedroom_list.append(None)
    try:
        if listing_soup.find('div', class_ = 'column-label', text = 'Bathrooms'):
            bathroom_list.append(int(listing_soup.find('div', class_ = 'column-label', text = 'Bathrooms').find_next_sibling('div').find(class_ = 'priv').contents[0]))
        else:
            bathroom_list.append(None)
    except:
        bathroom_list.append(None)
    try:
        if listing_soup.find('div', class_ = 'column-label', text = 'Basement'):
            basement_list.append(str(listing_soup.find('div', class_ = 'column-label', text = 'Basement').find_next_sibling('div').find(class_ = 'priv').contents[0]))
        elif listing_soup.find('div', class_ = 'column-label', text = 'Basement Status'):
            basement_list.append(str(listing_soup.find('div', class_ = 'column-label', text = 'Basement Status').find_next_sibling('div').find(class_ = 'priv').contents[0]))
        else:
            basement_list.append(None)
    except:
        basement_list.append(None)
    try:
        if listing_soup.find('div', class_ = 'column-label', text = 'Size'):
            lot_size_list.append(str(listing_soup.find('div', class_ = 'column-label', text = 'Size').find_next_sibling('div').find(class_ = 'priv').contents[0]))
        else:
            lot_size_list.append(None)
    except:
        lot_size_list.append(None)
    try:
        if listing_soup.find('div', class_ = 'column-label', text = 'Total Finished Area'):
            sqft_list.append(str(listing_soup.find('div', class_ = 'column-label', text = 'Total Finished Area').find_next_sibling('div').find(class_ = 'priv').contents[0]).replace(' sqft',''))
        else:
            sqft_list.append(None)
    except:
        sqft_list.append(None)
    try:       
        if listing_soup.find('div', class_ = 'column-label', text = 'Parking Space (A)'):
            parking_list.append(str(listing_soup.find('div', class_ = 'column-label', text = 'Parking Space (A)').find_next_sibling('div').find(class_ = 'priv').contents[0]))
        elif listing_soup.find('div', class_ = 'column-label', text = 'Parking Space (B)'):
            parking_list.append(str(listing_soup.find('div', class_ = 'column-label', text = 'Parking Space (B)').find_next_sibling('div').find(class_ = 'priv').contents[0]))
        elif listing_soup.find('div', class_ = 'column-label', text = 'Parking Space (C)'):
            parking_list.append(str(listing_soup.find('div', class_ = 'column-label', text = 'Parking Space (C)').find_next_sibling('div').find(class_ = 'priv').contents[0]))
        else:
            parking_list.append(None)
    except:
        parking_list.append(None)
    try:
        if listing_soup.find(property="place:region"):
            province_list.append(str(listing_soup.find(property="place:region")['content']))
        else:
            province_list.append(None)
    except:
        province_list.append(None)
    try:
        if listing_soup.find(property="place:locality"):
            city_list.append(str(listing_soup.find(property="place:locality")['content']))
        else:
            city_list.append(None)
    except:
        city_list.append(None)
    try:
        if listing_soup.find(property="place:street_address"):
            address_list.append(str(listing_soup.find(property="place:street_address")['content']))
        else:
            address_list.append(None)
    except:
        address_list.append(None)
    try:
        if listing_soup.find(property="place:location:longitude"):
            long_list.append(float(listing_soup.find(property="place:location:longitude")['content']))
        else:
            long_list.append(None)
    except:
        long_list.append(None)
    try:
        if listing_soup.find(property="place:location:latitude"):
            lat_list.append(float(listing_soup.find(property="place:location:latitude")['content']))
        else:
            lat_list.append(None)
    except:
        lat_list.append(None)
    try:
        if listing_soup.find(class_= 'table-price xs-col-3 sm-col-2 xs-text-right'):
            price_list.append(float(listing_soup.find(class_= 'table-price xs-col-3 sm-col-2 xs-text-right').contents[0].replace(',', '').replace('$','')))
        else:
            price_list.append(None)
    except:
        price_list.append(None)



dict_zolo = {
    'mls_number':mls_list,
    'address':address_list,
    'city':city_list,
    'province':province_list,
    'number_of_rooms':bedroom_list,
    'number_of_bathrooms':bathroom_list,
    'parking_info':parking_list,
    'basement':basement_list,
    'price':price_list,
    'property_type':property_type_list,
    'attachement':attachement_list,
    'ownership':ownership_list,
    'sqft':sqft_list,
    'lot_size_acres':lot_size_list,
    'year_built':year_built_list,
    'listing_date':listing_date,
    'latitude':lat_list,
    'longitude':long_list,
    'latitude': lat_list,
    'listing_link':listing_links,
    'broker':broker_list
}


df_zolo = pd.DataFrame.from_dict(dict_zolo)

df_zolo.dropna(subset = ['mls_number','address', 'number_of_bathrooms','number_of_bathrooms','price','latitude', 'longitude','sqft'], inplace= True)

df_zolo = df_zolo.assign(is_active = True,days_on_market = np.nan)
df_zolo['listing_date'] = pd.to_datetime(df_zolo['listing_date'])


df_zolo.drop_duplicates(subset=['mls_number'],inplace = True)
df_zolo = df_zolo[df_zolo['province']=='PE']
df_zolo['attachement'].fillna('Attached',inplace=True)
df_zolo['days_on_market'] = df_zolo['listing_date'].apply((lambda x: days_on_market_calc(x)))


df_1 = df_zolo[['mls_number', 'address', 'city', 'province', 'number_of_rooms',
       'number_of_bathrooms', 'parking_info', 'basement', 'price',
       'property_type', 'attachement', 'ownership', 'sqft', 'lot_size_acres',
       'year_built', 'listing_date','days_on_market', 'latitude', 'longitude', 'listing_link',
       'broker', 'is_active']]


df_temp = df_1[['mls_number','lot_size_acres']].dropna()
df_temp['lot_size_acres'] = df_temp['lot_size_acres'].apply(lambda x: x.replace('1/2','.25'))
df_temp['lot_size_acres'] = df_temp['lot_size_acres'].apply(lambda x: lot_size_extract(x)) 
df_temp['lot_size_acres'] = df_temp['lot_size_acres'].astype(str)
df_temp = df_temp[~df_temp['lot_size_acres'].str.contains(r'[A-Za-z]')]
df_temp['lot_size_acres'] = df_temp['lot_size_acres'].astype(float)
df_merge = df_1.merge(df_temp,left_on = 'mls_number',right_on ='mls_number',how = 'left')



df_final = df_merge[['mls_number', 'address', 'city', 'province', 'number_of_rooms',
       'number_of_bathrooms', 'parking_info', 'basement', 'price',
       'property_type', 'attachement', 'ownership', 'sqft', 'lot_size_acres_y',
       'year_built', 'listing_date', 'days_on_market', 'latitude', 'longitude',
       'listing_link', 'broker', 'is_active']]

df_final.rename(columns={'lot_size_acres_y':'lot_size_acres'},inplace = True)


unique_id_check(df_final)
active_listings_insert(df_final)
active_listing_update()
days_on_market_update()
spatial_update()
