import sqlite3
from bs4 import BeautifulSoup
import requests
import pandas as pd
from datetime import datetime
from urllib.parse import unquote
from functools import reduce
import yagmail

dist_list = ["adam.grbac@gmail.com","biancathompson77@gmail.com"]

def htmlify(df):
    output = "<ul>"
    for row in df.to_dict(orient="records"):
        output+= f"<li>({row['severity']}) {row['data_location']}, {row['data_suburb']} on {row['data_datetext']} between {row['data_timetext']}</li>"
    output += "</ul>"
    return output
        

yag = yagmail.SMTP("adam.grbac@gmail.com",oauth2_file="oauth2_file.json")

# Open DB Connection
con = sqlite3.connect("contact_tracing.db")

cur = con.cursor()

max_time = [row for row in cur.execute("SELECT max(data_added) FROM contact_tracing")][0][0]

cur.execute("""
    CREATE TABLE IF NOT EXISTS contact_tracing (
       severity varchar(256),
       data_date timestamp,
       data_location varchar(256),
       data_lgas varchar(256),
       data_address varchar(256),
       data_suburb varchar(256),
       data_datetext varchar(256),
       data_timetext varchar(256),
       data_added  timestamp,
       updated_flag boolean
    );
""")

# Get contact tracing page HTML
res = requests.get("https://www.qld.gov.au/health/conditions/health-alerts/coronavirus-covid-19/current-status/contact-tracing")

# Parse page with bs4
page = BeautifulSoup(res.text, 'html.parser')

# Get tables elements from parsed paged
tables = page.find_all("div",{"class":"qh-table-wide"})

dfs = []

# Extract data from each table
for table in tables:
    table_name = table["id"].split("_")[1]
    
    # Skip non-qld locations
    if table["id"][:3] != "qld":
        continue
    
    # Rows
    data = []
    for row in table.tbody.find_all("tr"):
        data.append(row.attrs)
        
    df = pd.DataFrame(data)
    
    col_names = list(df.columns)
    
    
    df["severity"] = table_name
    df["data_date"] = pd.to_datetime(df["data-date"])
    df["data_location"] = df["data-location"].apply(unquote)
    df["data_address"] = df["data-address"].apply(unquote)
    df["data_suburb"] = df["data-suburb"].apply(unquote)
    df["data_datetext"] = df["data-datetext"].apply(unquote)
    df["data_timetext"] = df["data-timetext"].apply(unquote)
    df["data_added"] = pd.to_datetime(df["data-added"])
    df["updated_flag"] = False if "class" not in df.columns else df["class"].apply(lambda x: False if type(x) != list else "qh-updated" in x) 
    
    df = df.drop(col_names, axis=1)
    
    dfs.append(df)

df = pd.concat(dfs)

new_records = df[(df["data_added"] > max_time) & (df["updated_flag"] == 0)]
updated_records = df[(df["data_added"] > max_time) & (df["updated_flag"] == 1)]

if len(new_records) > 0 or len(updated_records) > 0:
    contents = []

    if len(new_records) > 0:
        contents.append("New Contact Tracing Locations added to the website:")
        contents.append(htmlify(new_records))
    if len(updated_records) > 0:
        contents.append("Updated Contact Tracing Locations added to the website:")
        contents.append(htmlify(updated_records))
      
    yag.send(to=dist_list, subject="New Contact Tracing Locations!", contents=contents)

    # Insert New Records
    new_records.to_sql("contact_tracing", con, if_exists="append", index=False)
    updated_records.to_sql("contact_tracing", con, if_exists="append", index=False)
else:
    print("No updates!")

con.close()
