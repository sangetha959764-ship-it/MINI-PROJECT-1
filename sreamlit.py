import streamlit as st
import pandas as pd
import pymysql
from sqlalchemy import create_engine

# ---------------------------

# MySQL Database Connection
# ---------------------------
host="localhost",
user="root",
password="12345",
database="project1"
#---------------------------
engine = create_engine("mysql+pymysql://root:12345@localhost/project1")

# ---------------------------
# SQL Queries for 30 Tasks
# ---------------------------


quries = {
    #----- Magnitude & Depth-----

'1.Top 10 strongest earthquakes (mag)':
"""Select * From earthquakes order by mag DESC limit 10;""",
'2.Top 10 deepest earthquakes (depth_km)':
 """Select * From earthquakes order by depth_km DESC limit 10;""",
'3.Shallow earthquakes < 50 km and mag > 7.5':
"""select * from earthquakes where depth_km < 50 and mag>7.5;""",
'4.Average depth per continent':
 """select'Continent not available in DB' AS note;""",
'5.Average magnitude per magnitude type (magType)':
"""select AVG(mag - mag-type) as average_mag_type
from earthquakes
group by mag-type;""",
   
   #------Time Analysis-----

'6.Year with most earthquakes':
"""select year as most_eq_year,count(*) as total from earthquakes
group by year
order by total desc limit 1;""",
'7.Month with highest number of earthquakes':
"""select month as month_high_eq,count(*) as total from earthquakes
group by month
order by total desc limit 1;""",
'8.Day of week with most earthquakes':
"""select day_of_week as day_week_eq,count(*) as total from earthquakes
group by day_of_week
order by total desc;""",
'9.Count of earthquakes per (hour of day)':
"""select day as hour_of_day,count(*) from earthquakes
group by day 
order by day;""",
'10.Most active reporting network (net)':
"""select net ,count(*) as total from earthquakes
group by net
order by total desc limit 1;""",
 
  #-----Casualties & Economic Loss--------

'11.Top 5 places with highest casualties':
"""select place as place_high_casualties from earthquakes
group by place
order by place_high_casualties desc
limit 5;""",
'12.Total estimated economic loss per continent':
"""select'Continent not available in database' as note;""",
'13.Average economic loss by alert level':
"""select 'alert column not avilable in database' as note;""",
  
  #--------Event Type & Quality Metrics-----

'14.Count of reviewed vs automatic earthquakes (status)':
"""select status ,count(*) as review_automatic_count from earthquakes
group by status;""",
'15.Count by earthquake type (type)':
"""select type , count(*) as eq_type_count from earthquakes
group by type;""",
'16.Number of earthquakes by data type (types)':
"""select types ,count(*)as num_of_data_type from earthquakes
group by types;""",
'17.Average RMS and gap per continent':
"""SELECT 'Continent mapping not available' AS note;""",
'18.Events with high station coverage (nst > threshold)':
"""select * from earthquakes nst where nst>50;""",
  
  #------sunamis & Alerts-------

'19.Number of tsunamis triggered per year':
"""select year as year,count(*) AS tsunamis from earthquakes
where tsunami = 1
group by year;""",  
'20.Count earthquakes by alert levels':
"""select 'alert column not available in database' as note;""", 

#-----Seismic Pattern & Trends Analysis-----

'21.Top 5 countries with highest avg magnitude (past 10 yrs)':           	
"""select place,avg(mag) as avg_magnitude from earthquakes
group by place
order by avg_magnitude desc limit 5;""",
'22.Find countries experienced both shallow and deep earthquakes same month':
  """select place from earthquakes 
  group by place,year,month
  having sum(depth_km < 70) > 0 and sum(depth_km > 300) > 0;""",
'23.Compute the year-over-year growth rate':
"""select year,total,lag(total) over (order by year) AS previous_year,
round(((total - lag(total) over (order by year)) / lag(total) over (order by year)) * 100, 2) as growth_rate
from (select year as year, count(*) as total from earthquakes
	  group by year ) as yearly;""",
'24.List the 3 most seismically active regions by combining both frequency and average magnitude':
"""Select place,
count(*) as frequency,
avg(mag) as avg_mag,(count(*) * avg(mag)) as score
from earthquakes
group by place
order by score desc
limit 3;""",

#-------Depth, Location & Distance-Based  Analysis-----

'25.average depth of earthquakes within ±5° latitude':
"""select place, avg(depth_km) as avg_depth_km
from earthquakes
where latitude between -5 AND 5
group by place;""",
'26.Countries with highest shallow-to-deep ratio':
"""select place,
sum(depth_km < 70) AS shallow,
sum(depth_km > 300) AS deep,
SUM(depth_km < 70) / nullif(sum(depth_km > 300), 0) as ratio
from earthquakes
group by place
order by ratio desc;""",
'27.Avg magnitude difference (tsunami vs non-tsunami)':
"""select
(select avg(mag) from earthquakes where tsunami = 1) as tsunami_avg,
(select avg(mag) from earthquakes where tsunami = 0) as no_tsunami_avg,
(select avg(mag) from earthquakes where tsunami = 1)-
(select avg(mag) from earthquakes where tsunami = 0) as difference;""",  
'28.Events with lowest data reliability (gap & rms)':
"""select * from earthquakes
order by gap desc, rms desc limit 20;""",
'30.Regions with highest deep-focus EQ (>300 km)':
"""select place,count(*) as deep_events
from earthquakes
        where depth_km > 300
        group by place
       order by deep_events desc;"""  


 }        

print('susceess=', quries)

# --------------------------------------
# Streamlit UI
# --------------------------------------
st.title("🌍 Earthquake Data Analysis Dashboard")
st.write("Select any problem statement (1-40) to run the corresponding SQL query.")

# Dropdown
task = st.selectbox("Choose Task Number", list(quries.keys()))

# Run button
if st.button("Run Query"):
    query = quries[task]
    df = pd.read_sql(query, engine)
    
    st.subheader(f"Results for: {task}")
    st.dataframe(df, use_container_width=True)


