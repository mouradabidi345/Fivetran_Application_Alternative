

# Python 3
import http.client
import numpy as np
import mimetypes
import base64
import json
#import keyring
import datetime
import csv
import pandas as pd
import sys
from io import StringIO
import pyodbc
# import sqlalchemy
import math
import urllib.parse 
import re
import http.client
import json
import pandas as pd






client_id =  "xxxxxxxxxx"
client_secret =  "xxxxxxxxx"

client_id_UrlEncoded = urllib.parse.quote_plus(client_id)
client_secret_UrlEncoded = urllib.parse.quote_plus(client_secret)


AUTHCODE = base64.b64encode(
    (client_id_UrlEncoded  + ":" + client_secret_UrlEncoded).encode()
  ).decode()


#SERVICENAME = APPLICATION + "@" + VENDOR
FILEPATH = "cxOneToken.json"
API_VERSION = "v15.0"
PROGRESS = {
  0: '|',
  1: '/',
  2: '-',
  3: '\\',
}


def CreateNewToken() -> dict:
  """
  Obtains a new token from cxOne, returns the values as a dict.
  """
  print("Generating new token...")
  
  ACCESS_KEY_ID = "xxxxxxxx"
  username = urllib.parse.quote_plus(ACCESS_KEY_ID)

  SECRET_ACCESS_KEY ="xxxxxxx"
  password = urllib.parse.quote_plus(SECRET_ACCESS_KEY)

  # Get connection to token base URL
  conn = http.client.HTTPSConnection("cxone.niceincontact.com")
  # Create headers and payload
  headers = {
      "Authorization" : "Basic " + AUTHCODE,
      "Content-type" : "application/x-www-form-urlencoded"
      #"Cookie" : "BIGipServerpool_api=",
  }


  payload = f"grant_type=password&username={username}&password={password}"
  
  

  # Send request and process response
  conn.request("POST", "/auth/token", payload, headers)
  response = conn.getresponse()
  data = response.read()


  
  data.decode("utf-8")
  data = json.loads(data)
  cxOneToken= data[ "access_token"]
  return cxOneToken





def ParseSaveTokenResponse(data: str) -> dict:
  """
  Receives response from either a token request or a token refresh in 
  a string. It saves the response as a json file, then returns a dictionary 
  """
  cxOneToken = json.loads(data)
  start = datetime.datetime.utcnow()
  # Set expiration to 5 seconds before to allow for at least 1 API call
  expirey = start + datetime.timedelta(
    seconds = cxOneToken['expires_in'] - 5)
  cxOneToken['resource_server_base_uri'] += "services/" + API_VERSION + "/"
  cxOneToken['start'] = start.strftime('%Y-%m-%d %H:%M:%S.%f')
  cxOneToken['expirey'] = expirey.strftime('%Y-%m-%d %H:%M:%S.%f')
  with open(FILEPATH, 'w') as f:
    json.dump(cxOneToken, f, indent = 4)
  return cxOneToken


def UrlParser(wholeUrl: str) -> dict:
      dblSlashIdx = wholeUrl.find('//')
      protocol = wholeUrl[: dblSlashIdx - 1]
      domainWithPath = wholeUrl[dblSlashIdx + 2:]
      sglSlashIdx = domainWithPath.find('/')
      domain = domainWithPath[:sglSlashIdx]
      path = domainWithPath[sglSlashIdx:]
      return {
       'protocol' : protocol,
        'domain' : domain,
        'path' : path,
  }


def PrintProgress(text: str, endProgress = False) -> str:
  returnText = text
  text = ''
  while len(returnText) > 80:
    text += returnText[:80] + '\n'
    returnText = returnText[80:]
  text += '\r' + returnText
  if endProgress:
    text += '\n'
  sys.stdout.write(text)
  sys.stdout.flush()
  return returnText
  

def StartReportingJob(reportId: str) -> str:
  """
  Starts a reporting job, returns jobId in string format. While the jobId
  appears to be an int, the return type remains a string in case this changes
  in the future.
  """
  # Check if token exists, refresh/ generate new token as needed
#   cxOneToken = RetrieveCheckToken()
  cxOneToken =CreateNewToken() 
  
  print("Starting job to run report number " + reportId + "...")

  #parsedUrl = UrlParser(cxOneToken['resource_server_base_uri'])
  parsedUrl = "/inContactAPI/services/v15.0/"
  targetPath = parsedUrl + "report-jobs/" + reportId

  conn = http.client.HTTPSConnection("api-c58.nice-incontact.com")


  filetype =  'CSV'
  includeHeaders = 'true'
  deleteAfter= 7
  
  #payload = 'filetype=CSV&includeHeaders=true&deleteAfter=7'
  payload = f"filetype={filetype}&includeHeaders={includeHeaders}&deleteAfter={deleteAfter}"

  headers = {
    'Authorization': 'Bearer ' + cxOneToken,
    'Content-Type': 'application/x-www-form-urlencoded',
    "Cookie" : "AWSALB=Y8GCOf+CsOdQ6YWuAO7cSGi5hSgkQEe7yAAXYdAh3alv3L4k5I/zAMqFeoJ0M4z9PUsn+Py2B4K0gcpsQAKJYV2QfYX/7Xg8ljYTKLvgwKYsFda+06GoR+WN41aU; AWSALBCORS=Y8GCOf+CsOdQ6YWuAO7cSGi5hSgkQEe7yAAXYdAh3alv3L4k5I/zAMqFeoJ0M4z9PUsn+Py2B4K0gcpsQAKJYV2QfYX/7Xg8ljYTKLvgwKYsFda+06GoR+WN41aU",
     "Accept" : "*/*",
     'Connection' : 'keep-alive'
  }
  conn.request("POST", targetPath, payload, headers)
  #conn.request("POST", "/inContactAPI/services/v15.0/report-jobs/1097", payload, headers)
  response = conn.getresponse()
  
  data = response.read()
  data.decode("utf-8")
  
  data1 = json.loads(data)
  
  return data1['jobId']



def GetReportingJobInfo(jobId: str) -> dict:
  """
  Keeps checking running job every 1/10th second until either it returns the report URL or
  it fails. Times out after 10 minutes
  """
  # Check if token exists, refresh/ generate new token as needed
  #cxOneToken = RetrieveCheckToken()
  cxOneToken =CreateNewToken()
  print("Checking job status...")
  #parsedUrl = UrlParser(cxOneToken['resource_server_base_uri'])
  parsedUrl = "/inContactAPI/services/v15.0/"
  targetPath = parsedUrl + "report-jobs/" + jobId
  #conn = http.client.HTTPSConnection(parsedUrl['domain'])
  conn = http.client.HTTPSConnection("api-c58.nice-incontact.com")
  payload = {}
  headers = {
    'Authorization': 'Bearer ' + cxOneToken,
    'Content-Type': 'application/json',
    "Cookie" : "BIGipServerpool_api=",
    "Accept" : "*/*",
    'Connection' : 'keep-alive'
  }
  jobState = ''
  data = {}
  statusCode = 200
  now = start = datetime.datetime.utcnow()
  timeout = start + datetime.timedelta(minutes = 5)
  outNum = 0
  print("Time\t\t\tStatus Code\t\tJob State")
  while jobState != "Finished" \
    and statusCode < 300 \
    and statusCode >= 200 \
    and now < timeout:
    # First, print out the progress animation, then add 1 the progress number
    PrintProgress(PROGRESS.get(outNum))
    outNum += 1
    outNum = outNum % 4
    conn.request("GET", targetPath, payload, headers)
    #conn.request("GET", "/inContactAPI/services/v15.0/report-jobs/36971", payload, headers)
    response = conn.getresponse()
    data = response.read()
    data = json.loads(data)
    if jobState != data['jobResult']['state']:
      jobState = data['jobResult']['state']
      consoleStr = now.strftime('%Y-%m-%d %H:%M:%S') + '\t'
      consoleStr += str(response.getcode()) + '\t\t\t' + jobState
      print('\r' + consoleStr)
    # time.sleep(.1)
    now = datetime.datetime.utcnow()
  print("Job Completed")
  return data['jobResult']

def GetFinishedReport(jobResult: dict) -> pd.DataFrame:
  """
  Downloads the report that was just generated. Puts the file into a pandas dataframe,
  removes null columns, and saves the file to the disk. Returns a dataframe that then can
  be used to insert rows into database.
  """
  # Check if token exists, refresh/ generate new token as needed
  cxOneToken = CreateNewToken()
  print("Retrieving File...")
  parsedUrl = UrlParser(jobResult['resultFileURL'])
  conn = http.client.HTTPSConnection(parsedUrl['domain'])
  payload = ''
  headers = {
    'Authorization': 'Bearer ' + cxOneToken,
    'Content-Type': 'application/json',
    "Cookie" : "BIGipServerpool_api=",
    "Accept" : "*/*",
    'Connection' : 'keep-alive'
    }
  conn.request("GET", parsedUrl['path'], payload, headers)
  response = conn.getresponse()
  #pp.pprint(res.getheaders())
  rawData = response.read()
  rawData = json.loads(rawData)
  fileName = rawData['files']['fileName'].replace(' ', '_')
  rawFile = rawData['files']['file']
  rawFile = rawFile.encode()
  rawFile = base64.decodebytes(rawFile)
  rawFile = rawFile.decode('utf-8-sig')
  rawFile = rawFile.replace('\r\n\r\n', '\r\n').replace('\r\n', '\n')
  rawFile = rawFile[:-1]
  rawFile = rawFile[:rawFile.rfind('\n')]
  rawFile = StringIO(rawFile)
  fileDf = pd.read_csv(rawFile)
  fileDf = fileDf.dropna(axis = 'columns', how = 'all')
  fileDf.columns = fileDf.columns.str.replace(' ', '_')
  d= datetime.datetime.today()
  sun_offset = (d.weekday() - 6) % 7
  sunday_same_week = d - datetime.timedelta(days=sun_offset)
  td = datetime.timedelta(days=7)
  Report_Start_Date = (sunday_same_week - td).strftime("%Y-%m-%d")
  fileDf.insert(0,"Report_Start_Date",Report_Start_Date)
  fileNameList = fileName.split("_")
  fileNameList[-1] = (sunday_same_week - td).strftime("%Y-%m-%d.csv")
  fileName = '_'.join(fileNameList)
  with open(fileName, 'w') as f:
    fileDf.to_csv(f, line_terminator = '\n', index = False)
  print("File retrieval success")
  return fileDf

if __name__ == "__main__":
  reportId = '1126'
  jobId = StartReportingJob(reportId)
  print("Job started, Job ID: " + jobId)
  jobResult = GetReportingJobInfo(jobId)
  print(jobResult)
  jobResult['fileName'] = jobResult['fileName'].replace(" ", "_")
  print("Filename: " + jobResult['fileName'])
  fileDf = GetFinishedReport(jobResult)
  fileDf['natural_key'] =  str(fileDf['Report_Start_Date']) + str(fileDf['Agent_Name']) + str(fileDf['Media_Type_Name']) + str(fileDf['Contact_ID']) + str(fileDf['Contact_Start_Date_Time']) 
  fileDf['natural_key'] =  fileDf['Report_Start_Date'].map(str) + fileDf['Agent_Name'].map(str) + fileDf['Media_Type_Name'].map(str) + fileDf['Contact_ID'].map(str) + fileDf['Contact_Start_Date_Time'].map(str)
    #remove any space
  fileDf['natural_key']= fileDf['natural_key'].str.replace(' ', '')
  print(fileDf)

  columns_list = list(fileDf.columns)
  
  View_columns = ','.join(columns_list)

  columns_list_enhanced = []
  for i in columns_list:
    i1 ='VALUE'+ ':' + i + '::string AS ' + i
    columns_list_enhanced.append(i1)



  columns_query = ','.join(columns_list_enhanced)
  print(columns_query)



  print(fileDf.dtypes)

  jsondata =  fileDf.to_json(orient='index')
#   jsondata1 = jsondata[0]
  


  test = ''' create or replace transient table MY_TABLE ( MY_DICT variant);
             insert into  MY_TABLE  select PARSE_JSON($$'''  + jsondata + """$$);
             


             create or replace view Final_view (""" + View_columns + """) as 

             with RAW AS (
                 select t0.value
                  from "DEMO_DB"."PUBLIC"."MY_TABLE" ,
                  lateral flatten(input=>my_dict) as t0 ) 

             select """ + columns_query + """ FROM RAW ;
             CREATE OR REPLACE TABLE LAST_TABLE_FROM_jsON AS 
                SELECT * FROM Final_view;""" ##+ str(tuplenew)
  
  
  list = re.split(r'\.\.\.', test)
  print(list)
  query = list[0]
  print(query)


  conn = http.client.HTTPSConnection("gc55340.west-us-2.azure.snowflakecomputing.com")
  payload = json.dumps({
  "statement": query,
  "timeout": 60,
  "resultSetMetaData": {
    "format": "json"
  },
  "database": "DEMO_DB",
  "schema": "PUBLIC",
  "warehouse": "COMPUTE_WH",
  "parameters": {
    "MULTI_STATEMENT_COUNT": "0"
  }
})
  headers = {
  'Authorization': 'Bearer xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',
  'Content-Type': 'application/json',
  'User-Agent': 'myApplicationName/1.0',
  'Accept': 'application/json'
  
}
  conn.request("POST", "/api/v2/statements", payload, headers)
  res = conn.getresponse()
  data = res.read()
  print(data)


