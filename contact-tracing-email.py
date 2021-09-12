import sqlite3
import requests
import pandas as pd
import yagmail
import yaml
import utils
from bs4 import BeautifulSoup

# Load email config
with open("email_config.yml", "r") as f:
    email_config = yaml.safe_load(f)

# Setup Email details
yag = yagmail.SMTP(email_config["sender"], oauth2_file="oauth2_file.json")

# Open DB Connection
con = sqlite3.connect("contact_tracing.db")
cur = con.cursor()

# Get time of latest update & create table if missing
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
tables = page.find_all("div", {"class": "qh-table-wide"})

# Create empty list of dfs to merge later
dfs = []

# Extract data from each table
for table in tables:
    # Get name of table for severity column
    table_name = table["id"].split("_")[1]

    # Skip non-qld locations
    if table["id"][:3] != "qld":
        continue

    # Convert <tr> attributes to list of dicts
    data = []
    for row in table.tbody.find_all("tr"):
        data.append(row.attrs)

    # Convert list of dicts to DataFrame
    df = pd.DataFrame(data)

    # Clean dataframe for easier formatting
    df = utils.clean_dataframe(df, table_name)

    # Append df to list, to be merged later
    dfs.append(df)

# Merge dfs into one df
if len(dfs) == 0:
    print("No Exposure Sites!")
    quit()

df = pd.concat(dfs)

# Get records that have appeared since last entry in the database
# Separate these rows into new rows and updated rows based on updated_flag
new_records = df[(df["data_added"] > max_time) & (df["updated_flag"] == 0)]
updated_records = df[(df["data_added"] > max_time) & (df["updated_flag"] == 1)]

# If there are any new / updated rows, process and email to dist list
if len(new_records) > 0 or len(updated_records) > 0:

    # Email body
    contents = []

    # Create upto two sections depending on presences of new/updated records
    if len(new_records) > 0:
        contents.append("New Contact Tracing Locations added to the website:")
        contents.append(utils.htmlify(new_records))
    if len(updated_records) > 0:
        contents.append("Updated Contact Tracing Locations added to the website:")
        contents.append(utils.htmlify(updated_records))
    
    contents.append('<br><br><br>If you wish to unsubscribe from this service, click <a href="https://covidmailer.au.ngrok.io/unsubscribe">here</a> and fill out the form.')

    # Send email to dist list
    yag.send(bcc=email_config["dist_list"], subject="New QLD Contact Tracing Locations!", contents=contents)

    # Insert new records into database to mark them as processed
    new_records.to_sql("contact_tracing", con, if_exists="append", index=False)
    updated_records.to_sql("contact_tracing", con, if_exists="append", index=False)
else:
    # For logging purposes
    print("No updates!")

# Close DB connection
con.close()
