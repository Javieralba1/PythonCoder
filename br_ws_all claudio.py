"""Hello Analytics Reporting API V4."""
import socket
#from googleapiclient import discovery
socket.setdefaulttimeout(600)  # set timeout to 10 minutes
from apiclient.discovery import build
import sqlalchemy
from oauth2client.service_account import ServiceAccountCredentials
from pandas.io import sql
import MySQLdb
import pandas as pd
from datetime import datetime
from datetime import timedelta
import time
import sys

#con = MySQLdb.connect("localhost", 'vecindad', 'igor100564', "karvi")

SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
KEY_FILE_LOCATION = '../../gaproyect-1034-74974f6e4fd8.json'
VIEW_ID = '231602508'

def initialize_analyticsreporting():
  """Initializes an Analytics Reporting API V4 service object.
  Returns:
    An authorized Analytics Reporting API V4 service object."""
  credentials = ServiceAccountCredentials.from_json_keyfile_name(KEY_FILE_LOCATION, SCOPES)
  # Build the service object.
  analytics = build('analyticsreporting', 'v4', credentials=credentials)
  return analytics

def get_report(analytics,day,day1):
  """Queries the Analytics Reporting API V4.
  Args:
    analytics: An authorized Analytics Reporting API V4 service object.
  Returns:
    The Analytics Reporting API V4 response.
  """
  return analytics.reports().batchGet(
      body={
        'reportRequests': [
        {
          'viewId': VIEW_ID,
          'pageSize': 10000,
          'dateRanges': [{'startDate': day, 'endDate': day1}],
          'metrics': [{'expression': 'ga:totalEvents'},{'expression': 'ga:uniqueEvents'}],
          'dimensions': [{'name': 'ga:date'}],
          "dimensionFilterClauses": [
        {
          "operator":"AND",
          "filters": [
            {
              "dimensionName": "ga:eventCategory",
              "operator": "REGEXP",
              "expressions": ["certificado-contact_karvi-click-analytics_event"]
            },
            #{
             # "dimensionName": "ga:campaign",
              #"operator": "PARTIAL",
              #"expressions": ["usadoscertificados"]
            #}
          ]
        }
      ],   
          "orderBys": [
        {"fieldName": "ga:date"}
      ]
               
          }]
        }
  ).execute()

def print_response(response):
  val=[]
  #Extract Data
  #print (response.get('reports', []))
  for report in response.get('reports', []):
      columnHeader = report.get('columnHeader', {})
      #print (columnHeader)
      dimensionHeaders = columnHeader.get('dimensions', [])
      #print (dimensionHeaders)
      metricHeaders = columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])
      #print (metricHeaders)
      met=[]
      for metric in metricHeaders:
        met.append(metric.get('name'))
      #print (met)
      rows = report.get('data', {}).get('rows', [])
      columnas=dimensionHeaders+met
      for n, i in enumerate(columnas):
        columnas[n]=columnas[n].replace(":","_")
        '''columna=columna.replace(":","_")'''
      
      for i in rows:
        dimensions=i.get('dimensions',{})
        metrico=i.get('metrics',{})[0].get('values',{})
        valores=dimensions+metrico
        val.append(valores)
       # print(valores)
  df = pd.DataFrame()
 
  if len(val) != 1:
    df=pd.DataFrame(val,columns=columnas)
  else:
    df=pd.DataFrame(val,columns=columnas)
  print (df)
  from sqlalchemy import create_engine
  import pymysql
  #engine = create_engine('mysql+pymysql://vecindad:igor100564@localhost/karvi', echo=False)
  #df.to_sql(name='performance_br', con=engine, if_exists = 'append', index=False, dtype={
  #                 'ga_dimension2':  sqlalchemy.types.VARCHAR(255),
  #                 'ga_date': sqlalchemy.sql.sqltypes.DATE(), 
  #                 'ga_sourceMedium':  sqlalchemy.types.VARCHAR(255),
  #                 })             
  #df['ga_date'] = pd.to_datetime(df['ga_date'], format='%Y%m%d').dt.strftime('%Y-%m-%d')                    
  return df
  
 
def save_to_mysql(df, start_date):
  from oauth2client.service_account import ServiceAccountCredentials
  import gspread
  from df2gspread import df2gspread as d2g
  import numpy as np
  import pandas as pd
  googlesheet_domain = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
  #credentials = Credentials.from_service_account_file('python-googlesheets-api.json', scopes=googlesheet_domain)
  credentials = ServiceAccountCredentials.from_json_keyfile_name('../../python-googlesheets-api.json',googlesheet_domain)
  client = gspread.authorize(credentials)
  #service = build('sheets', 'v4', credentials=credentials)
  range = 'RAW - WS!A:C'
  googlesheet_key = "1_FBp2ak7A5JcOM71IVXN9DBGXuIYYhLixtYeSGn2MXk"
  service = build('sheets', 'v4', credentials=credentials)
  range = 'RAW - WS!A:C'
  googlesheet_key = "1_FBp2ak7A5JcOM71IVXN9DBGXuIYYhLixtYeSGn2MXk"
  service = build('sheets', 'v4', credentials=credentials)
  '''rows = service.spreadsheets().values().get(spreadsheetId=googlesheet_key, range=range).execute().get('values', [])
  last_row_id=0
  x=1
  for e in rows:
    print (e[0])
    print (df['ga_date'].iloc[0])
    if e[0] == df['ga_date'].iloc[0]:
      last_row_id=x
      break
    x=x+1
  if last_row_id == 0:
    last_row = rows[-1] if rows else None
    last_row_id = len(rows)
    last="A"+str(last_row_id+1)
    print(last)
  else:
    last="A"+str(last_row_id)
  print (last)  
 ''' 
  
  search_terms_sheet = "RAW - WS"
  d2g.upload(df, googlesheet_key, search_terms_sheet, credentials=credentials, col_names=False, row_names=False, clean=False,start_cell="a2")
  #sql.write_frame(df, con=con, name='br_paid_daily_stats',if_exists='append', flavor='mysql')'''
  

def main(day='today'):
  analytics = initialize_analyticsreporting()
  if day=='today':
    start_date=datetime.today().strftime('%Y-%m-%d')
    end_date=datetime.today().strftime('%Y-%m-%d')
    start_date="2021-05-01"
    #end_date="2021-09-30"
  if day=='yesterday':
    start_date=(datetime.today()-timedelta(days=1)).strftime('%Y-%m-%d')
    end_date=(datetime.today()-timedelta(days=1)).strftime('%Y-%m-%d')
    start_date="2021-0-27"
    end_date="2021-09-27"
  loop_date_start=datetime.strptime(start_date, '%Y-%m-%d')
  loop_date_end=datetime.strptime(end_date, '%Y-%m-%d')
  #while loop_date_start <= loop_date_end:
   # time.sleep(5)
   # print (loop_date_start.strftime('%Y-%m-%d'))
  response = get_report(analytics,start_date,end_date)
  save_to_mysql(print_response(response),start_date)
  #loop_date_start=loop_date_start + timedelta(days=1)
if __name__ == '__main__':
  if len(sys.argv) == 2:
    day=(sys.argv[1])
  else:
    day="today"
  main(day)


