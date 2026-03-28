#------ import statement-----
import requests
import pandas as pd
from datetime import datetime
import re
import pymysql
import sqlalchemy as sqlalchemy
from sqlalchemy import create_engine

#Dataset retrival------
url = "https://earthquake.usgs.gov/fdsnws/event/1/query"

all_records = []
start_year = datetime.now().year - 5
end_year = datetime.now().year


for year in range(start_year, end_year + 1):
    for month in range(1, 13):
        start_date = f"{year}-{month:02d}-01"
        if month == 12:
            end_date = f"{year+1}-01-01"
        else:
            end_date = f"{year}-{month+1:02d}-01"

        params = {
    "format": "geojson",
    "starttime": start_date,
    "endtime": end_date,
    "minmagnitude": 3
     }


        response = requests.get(url, params=params)
        if response.status_code != 200:
            print(f"{start_date}: {response.text[:200]}")
            continue

        try:
            data = response.json()
        except Exception as e:
            print(f"{start_date}: {e}")
            continue

        for f in data["features"]:
            p = f["properties"]
            g = f["geometry"]["coordinates"]
            all_records.append({
                "id": f.get("id"),
                "time": pd.to_datetime(p.get("time"), unit="ms"),
                "updated": pd.to_datetime(p.get("updated"), unit="ms"),
                "latitude": g[1] if g else None,
                "longitude": g[0] if g else None,
                "depth_km": g[2] if g else None,
                "mag": p.get("mag"),
                "mag-type": p.get("mag-type"),
                "place": p.get("place"),
                "status": p.get("status") ,
                "tsunami": p.get("tsunami"),
                "sig": p.get("sig"),
                "net": p.get("net"),
                "nst": p.get("nst"),
                "dmin": p.get("dmin"),
                "rms": p.get("rms"),
                "gap": p.get("gap"),
                "mag error": p.get("mag error"),
                "depth error": p.get("depth error"),
                "magnst": p.get("magnst"),
                "location source": p.get("location source"),
                "mag source": p.get("mag source"),
                "types": p.get("types"),
                "ids": p.get("ids"),
                "sources": p.get("sources"),
                "type": p.get("type"),

            })


df = pd.DataFrame(all_records)
df
print("Rows:", df.shape[0])
print("Columns:", df.shape[1])

#Data Preparation--------
data = 'earthquake.csv'
df.to_csv(data , index = False)
df = pd.read_csv(r'C:\Users\Niveditha.S\OneDrive\Desktop\mini project 1\earthquake.csv')
print('all columns =', df)
df['time'] = pd.to_datetime(df['time'])
df['updated'] = pd.to_datetime(df['updated'])

# clean text file-------
df['place'] = df['place'].str.extract(r',\s*([A-Za-z]+)$')
df['status'].fillna(df['status'].mode()[0], inplace=True)
df['net'].fillna(df['net'].mode()[0], inplace=True)
df['types'].fillna(df['types'].mode()[0], inplace=True)
df['ids'].fillna(df['ids'].mode()[0], inplace=True)
df['sources'].fillna(df['sources'].mode()[0], inplace=True)
df['type'].fillna(df['type'].mode()[0], inplace=True)
df['place'].fillna(df['place'].mode()[0], inplace=True)


# clean numeric file-------
df['mag'] = df['mag'].fillna(df['mag'].median()).astype(int)
df['depth_km'] = df['depth_km'].fillna(df['depth_km'].median()).astype(int)
df['nst'] = df['nst'].fillna(df['nst'].median()).astype(int)
df['dmin'] = df['dmin'].fillna(df['dmin'].median()).astype(int)
df['rms'] = df['rms'].fillna(df['rms'].median()).astype(int)
df['gap'] = df['gap'].fillna(df['gap'].median()).astype(int)
df['sig'] = df['sig'].fillna(df['sig'].median()).astype(int)
 
df['mag error'] = df['mag error'].fillna(0).astype(int)
df['depth error'] = df['depth error'].fillna(0).astype(int)
df['magnst'] = df['magnst'].fillna(0).astype(int)

df['mag-type'] = df['mag-type'].fillna(0).median()
df['location source'] = df['location source'].fillna(0).median()
df['mag source'] = df['mag source'].fillna(0).median()

#*Add Derived Columns **-----
df['Year'] = df['time'].dt.year
df['month'] = df['time'].dt.month
df['day'] = df['time'].dt.day
df['day_of_week'] = df['time'].dt.day_name()

df["depth_category"] = df["depth_km"].apply( lambda x: "Shallow" if x <70 else "Deep")
df["mag_category"] = df["mag"].apply(lambda x: 'strong' if x <4 else 'destructive')

#cleaned dataset(load csv file)-------
cleaned_data = 'earthquake data.csv'
df.to_csv(cleaned_data , index = False)
df_clean = pd.read_csv(r'C:\Users\Niveditha.S\OneDrive\Desktop\mini project 1\earthquake data.csv')
print(df_clean)

# CONNECT TO MYSQL----------
conn_pymysql = pymysql.connect(
    host="localhost",
    user="root",
    password="12345",
    database="project_1"
)
cursor_pymysql = conn_pymysql.cursor()
print("PyMySQL connection established!")

try:
 # Connecting to mysql using Parameters
 connection = pymysql.connect( host = 'localhost', user = 'root',
 password = '12345', database = 'project1',cursorclass=pymysql.cursors.DictCursor
 )
 print("connected to mysql success")
except Exception as e:
 print("connection failed",e)

#create table('earthquakes') and push to workbench
# push datas from vs to mysql table('earthquakes')


engine = create_engine("mysql+pymysql://root:12345@localhost/project1")
df_clean.to_sql(
 name='earthquakes', # table name
 con=engine,
 if_exists='append', # insert data (no overwrite)
 index=False # avoid DataFrame index column
 )
print("Data inserted successfully to mysql")


