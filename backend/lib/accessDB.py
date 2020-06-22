# accessDB.py

from cloudant.client import Cloudant
from cloudant.query import Query
import time
from pandas import DataFrame
from pandas import concat as concatDf
from pandas import read_json as jsonToDf
from functools import lru_cache
from functools import wraps
import io
import base64
from json2html import *
import json
import numpy as np
from flask import request

# >>> internal

# hard coded info with relation to data base
# wrapper for decorating functions
def dayToTime(func):
    @wraps(func)
    def function_wrapper(x):
        return func(np.pi*(x-20)/365)
    return function_wrapper

def heatingOff(func):
    @wraps(func)
    def function_wrapper(x):
        f = func(x)
        if (0 > f):
            return 0
        else:
            return f
    return function_wrapper

# function for modeling daily CO2 footprint based on specified parameters
_normalizeHeating_ = 1
@heatingOff
@dayToTime
def heating(x):
    return (np.cos(x)*np.cos(x) - 0.42)*_normalizeHeating_

def constPerYear(ts):
    return 1/365

def idFunc(ts):
    return ts

_normalizeHeating_ = 1 / sum([heating(x) for x in range(0, 365)])

_DAYS_PER_YEAR_ = 365
_NON_ITEM_KEYS_ = ['_id', '_rev', 'name', 'type']
_FIG_FUNCS_ ={'ts':idFunc, 'heating':heating, 'warmwater':constPerYear}


def lastUpdate(name):
    return  db[Query(db,  selector={'type': 'aux', 'name':name})()['docs'][0]['_id']]['lastUpdate']

def fieldsQuery(query, no_item_keys):
    dic = query(limit=1, skip=0)['docs'][0]
    for key in no_item_keys:
        del dic[key]
    return [*dic]


def getDb():

    # credential to cloudant c4c2020
    api_access = {
      "apikey": "nTFpLxd6ufHJrwzzUbPPd_2G9wqhIx0_Twa9LqQH691U",
      "host": "61d79bba-2f14-46da-b1a3-eacc3848d4ff-bluemix.cloudantnosqldb.appdomain.cloud",
      "iam_apikey_description": "Auto-generated for key 63a548a5-93c8-42d6-8eb2-ba4fc644dac7",
      "iam_apikey_name": "Service credentials-1",
      "iam_role_crn": "crn:v1:bluemix:public:iam::::serviceRole:Manager",
      "iam_serviceid_crn": "crn:v1:bluemix:public:iam-identity::a/0a7e44c4d4d86c8f9a2f8ff4cc400f87::serviceid:ServiceId-e871c59c-7908-45b0-93fb-9898a5b496b5",
      "url": "https://61d79bba-2f14-46da-b1a3-eacc3848d4ff-bluemix.cloudantnosqldb.appdomain.cloud",
      "username": "61d79bba-2f14-46da-b1a3-eacc3848d4ff-bluemix"
    }
    # client = Cloudant.iam(ACCOUNT_NAME, API_KEY, connect=True)
    client = Cloudant.iam(
        api_access['username'],
        api_access['apikey'],
        connect=True
    )
    # Create a database using an initialized client
    # The result is a new CloudantDatabase or CouchDatabase based on the client
    db = client.create_database('database')

    # You can check that the database exists
    if db.exists():
        print('SUCCESS!! Client object db is created.')
        db = client['database']
    else:
        print('FAIL!! No client object is created.')
        db = None
    return db

db = getDb()
# internal <<<

# get v1 landing page
def getV1Page():
    return Query(db,  selector={'type': 'image', 'image':'architecture' })(limit=1000, skip=0)['docs'][0]['data']


# get data for specified name from db as specified by queryDict which is defined by th user
@lru_cache(maxsize=32)
def getData(name, queryJson, update, beginTs=0, endTs=365):
    queryDict = json.loads(queryJson)
    df = DataFrame([{'Error': 'None'}])
    type = None
    if ('type' in queryDict):
        docType = queryDict['type']
        if (docType in ['fix', 'running']):
            fields = getFields(name, docType, update)
            if ('field' in queryDict):
                fields =list(set(queryDict['field'].split(',')).intersection(set(fields)))
            response = runQuery(docType, name, updateTs = update)
            df = DataFrame(response)
            if ('running' == docType):
                df.sort_values('ts', inplace=True)
                df = df[(df['ts'] > beginTs) & (df['ts'] < endTs)]
            df.drop([x for x in df.columns if x not in fields], axis=1, inplace=True)
        else:
            df = DataFrame([{'Error': 'Invalid type specified.'}])
    else:
        df = DataFrame([{'Error': 'No type specified.'}])
    return df

# prerequisit:
#    - Decorated function must return dataframe
# convert output of decorated function to json or html
def formater(func):
    @wraps(func)
    def function_wrapper(*args, **kwargs):
        df = func(*args, **kwargs)
        # sort if specified and field ts exists
        if ('sort' in request.args):
            by = request.args['sort']
            if (by in df.columns):
                df.sort_values(by=by, inplace=True)
        # convert to dataframe to json and if requested json to html if specified
        response = df.to_json(orient='records')
        if (('format' in request.args) and ('html' == request.args['format'])):
            response = json2html.convert(json = response)
        return response
    return function_wrapper

@lru_cache(maxsize=32)
def getFields(name, docType, updateTs):
    q = Query(db,  selector={'type': docType, 'name':name})
    return fieldsQuery(q, _NON_ITEM_KEYS_)


@lru_cache(maxsize=32)
def runQuery(docType, name, updateTs):
    time.sleep(0.3)
    fields = getFields(name, docType, updateTs)
    q = Query(db,  selector={'type': docType, 'name':name}, fields = fields)
    return q(limit=1000, skip=0)['docs']

# /* load all data from "name" and prepare panda dataframes for further usage */
def _queryData(name):
    # collect prerequisits for queries
    updateTs = lastUpdate(name)
    time.sleep(1)
    #handle fix data
    fixDic = runQuery('fix', name, updateTs)
    fixDfBuffer = DataFrame.from_dict(fixDic)
    time.sleep(1)
    # handle variable data
    varsDict = runQuery('running', name, updateTs)
    varDfBuffer = DataFrame.from_dict(varsDict).set_index('ts')
    varDfBuffer.sort_index(inplace=True)
    fixDfBuffer.sort_index(inplace=True)
    return fixDfBuffer, varDfBuffer

def queryData(name, beginTs, endTs):
    fixDfBuffer, varDfBuffer = _queryData(name)
    varDfBuffer = varDfBuffer.loc[beginTs:endTs,:]
    return fixDfBuffer, varDfBuffer

def getVarDf(name, beginTs, endTs):
    fixDf, dataByTs = queryData(name, beginTs, endTs)
    fields = set(dataByTs.columns.tolist()).intersection(set(fixDf.columns.tolist()))
    fixDf = fixDf.loc[:, fields]
    fixDf['weight'] = 'weight'
    fixDf = fixDf.set_index('weight')
    indexList = dataByTs.index.values.tolist()
    dataByItem = concatDf([dataByTs, fixDf], axis=0)
    varDataDict = {col:[dataByItem.loc['weight', col]* dataByItem.loc[ts, col]
                        for ts in range(beginTs, endTs+1) if ts  in indexList]
                   for col in dataByItem.columns.values.tolist()}

    varDataDict['ts'] = indexList
    varDataDf = DataFrame(varDataDict)
    varDataDf.set_index('ts', inplace=True) # weighted value
    return varDataDf, dataByTs

def fixDataResults(name, beginTs, endTs):
    fixDf, dataByTs = queryData(name, beginTs, endTs)
    fixFields = set(fixDf.columns.tolist()).difference(set(dataByTs.columns.tolist()))
    fixData = fixDf.loc[:, fixFields]
    colList = ['ts'] + fixData.columns.values.tolist()

    newFigFunc = {}
    for col in colList:
        if (col in _FIG_FUNCS_):
            newFigFunc[col] = _FIG_FUNCS_[col]
        else:
            newFigFunc[col] = constPerYear

    fixDataDf = DataFrame({col:[newFigFunc[col](ts)
                                for ts in range(beginTs, endTs)]
                           for col in colList})
    for col in fixData.columns:
        fixDataDf.loc[:, col] *= fixDf.loc[0, col]
    fixDataDfSums = DataFrame(fixDataDf.drop('ts',axis=1).sum()).T
    fixDataDf.set_index('ts', inplace=True)
    return fixDataDfSums, fixDataDf

