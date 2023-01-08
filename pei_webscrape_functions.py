import re
import pandas as pd
import geopandas as gpd
from shapely import wkt
from sqlalchemy import create_engine
import psycopg2
from datetime import datetime


def unique_id_check(df_x):
    engine = create_engine('postgresql+psycopg2://postgres:password@localhost/analysis')
    df_sql = pd.read_sql_query('SELECT mls_number FROM pei_real_estate_listings', engine)
    x = ~df_x['mls_number'].isin(df_sql['mls_number'])
    if len(df_x[x]) == 0:
        print('Notice: No new listings have been found.')
    else:
        df_x[x].to_sql('pei_real_estate_listings', engine, if_exists = 'append', index = False)
        print('Notice: pei_real_estate_listings table updated.')
    engine.dispose()




def active_listings_insert(df_x):
    engine = create_engine('postgresql+psycopg2://postgres:password@localhost/analysis')
    df_x[['mls_number','is_active']].to_sql('active_pei_real_estate_listings', engine, if_exists = 'replace', index = False)
    engine.dispose()



def active_listing_update():

    conn = psycopg2.connect(
    database = 'analysis',
    user = 'postgres',
    password = 'password',
    host = 'localhost',
    port = '5432'
    )
    
    conn.autocommit = True
    cursor = conn.cursor()
    sql = '''
                UPDATE pei_real_estate_listings
                SET is_active = (
                    CASE
                        WHEN pei_real_estate_listings.mls_number NOT IN (SELECT mls_number FROM active_pei_real_estate_listings) THEN FALSE
                        ELSE TRUE 
                    END 
                );
          '''
    cursor.execute(sql)
    conn.commit()
    conn.close()

def days_on_market_update():
    conn = psycopg2.connect(
    database = 'analysis',
    user = 'postgres',
    password = 'password',
    host = 'localhost',
    port = '5432'
    )
    
    conn.autocommit = True
    cursor = conn.cursor()
    sql = '''
            UPDATE pei_real_estate_listings
            SET days_on_market = (
                CASE
                    WHEN is_active = True THEN date_part('day',(now() - listing_date))
                    ELSE days_on_market
                END 
            );
          '''
    cursor.execute(sql)
    conn.commit()
    conn.close()



def days_on_market_calc(listing_date):
    current_date = datetime.today().strftime('%Y-%m-%d')
    l_date = listing_date.strftime('%Y-%m-%d')
    d1 = datetime.strptime(l_date, "%Y-%m-%d")
    d2 = datetime.strptime(current_date, "%Y-%m-%d")

    delta = d2 - d1
    return delta.days

def spatial_update():
    engine = create_engine('postgresql+psycopg2://postgres:password@localhost/analysis')
    df_listing = gpd.GeoDataFrame(pd.read_sql_query('SELECT crel.mls_number, ST_AsText(gcrel.geom_point) AS geometry FROM pei_real_estate_listings crel JOIN geom_pei_real_estate_listings gcrel ON crel.mls_number = gcrel.mls_number', engine))
    df_shp = gpd.read_file('/Users/omarosefau/Desktop/shape_files/communites/civic_comm_1.SHP/community_polygon.shp')

    df_listing['geometry'] = gpd.GeoSeries.from_wkt(df_listing['geometry'],crs="EPSG:4326")
    df_listing_1 = gpd.GeoDataFrame(df_listing, geometry='geometry')

    df_shp.to_crs(df_listing_1.crs, inplace=True)
    df_shp.set_geometry('geometry',inplace=True)

    df_shp_final = gpd.sjoin(df_listing_1,df_shp, how='left',predicate='within')
    
    df_shp_final = df_shp_final[['mls_number', 'UNIQUE_ID','COMM_NM', 'COUNTY']]
    df_shp_final['COMM_NM'] = df_shp_final['COMM_NM'].str.title()
    df_shp_final = df_shp_final.rename(columns={'UNIQUE_ID':'unique_id','COMM_NM':'community','COUNTY':'county'})


    df_sql = pd.read_sql_query('SELECT mls_number FROM pei_real_estate_spatial', engine)
    x = ~df_shp_final['mls_number'].isin(df_sql['mls_number'])

    if len(df_shp_final[x]) == 0:
        print('Notice: pei_real_estate_spatial is up to date.')
    else:
        df_shp_final[x].to_sql('pei_real_estate_spatial', engine, if_exists = 'append', index = False)
        print('Notice: pei_real_estate_spatial table updated.')
    engine.dispose()



def lot_size_extract(x):
    
    num = re.compile(r'[-+]?\d*\.\d+|\d+')
    pipe = re.compile(r'\|')
    mult = re.compile(r'x', flags = re.IGNORECASE)
    add = re.compile(r'\+')
    equal = re.compile(r'\=')
    sq = re.compile(r'sq', flags = re.IGNORECASE)
    try:
        if bool(pipe.search(x)):
            y = x.split('|')[0]
            y1 = num.findall(y)
            y2 = [float(i) for i in y1]
            if bool(mult.search(x)):
                acres = round((y2[0] * y2[1] / 43560),2)
                return(acres)
            elif bool(equal.search(x)):
                return max(y2)
            elif bool(add.search(x)):
                if len(y2) > 1:
                    return sum(y2)
                else:
                    return y2[0]
            elif bool(sq.search(x)):
                return round((y2[0] / 43560),2)
            else:
                return y2[0]
        else:
            z1 = num.findall(x)
            z2 = [float(i) for i in z1]
            if len(z2) > 1:
                return round((sum(z2) / len(z2)),2)
            elif len(z2) == 1:
                if z2[0] * 1 > 0:
                    return z2[0]
                else:
                    return None 
            else:
                return None       
    except:
        return None


def lot_size_regex(x):
    try:
        num = re.match(r'[-+]?\d*\.\d+|\d+',x).group(0)
        return num
    except:
        return None



