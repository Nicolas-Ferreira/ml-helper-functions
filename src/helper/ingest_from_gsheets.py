import boto3
import base64
from botocore.exceptions import ClientError
import pandas as pd
from datetime import datetime
import psycopg2
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from sqlalchemy import create_engine
from sqlalchemy.sql import text
import json



def getData(file, name):

    sheet = client.open(file)

    df = get_as_dataframe(
    	sheet.worksheet(name), 
    	parse_dates=True, dtype=str
    )

    return df


def parse_data(file, name):

    try:
        df = getData(file, name)
        df.dropna(
        	inplace=True, 
        	how='all'
        )
    except:
        df = pd.DataFrame()

    return df


# Secret name and Region
secretName = "secret_name"
secretRegion = "secret_region"

# Get credentials
user = get_secret(secretName, secretRegion)
user = json.loads(user)
redshift_endpoint = user['host']
redshift_user = user['username']
redshift_pass = user['password']
port = user['port']
dbname = user['dbname']

# Create Redshift conn
engine_string = "postgresql+psycopg2://%s:%s@%s:%d/%s" \
                % (redshift_user, redshift_pass, redshift_endpoint, port, dbname)
engine = create_engine(engine_string)

# Create scope
scope = ["https://spreadsheets.google.com/feeds", 
		 "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name("Api_Sheets.json",scope)
client = gspread.authorize(creds)

# Get data from Google Spreadsheet
df = parse_data("Filename", 'SheetName')

# Save data in Redshift
if not df.empty:    
    df.to_sql(
    	schema='schema',
		name='table',
		con=engine,
		if_exists='append',
		chunksize=100,
		index=False,
		method='multi'
	)
