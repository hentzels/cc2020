import lib 
from lib import formater
from lib import getData
from lib import getV1Page
from lib import getFigure
from lib import getCompare
from lib import figAsHtml
from lib import lastUpdate

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
from flask import request

from flask import jsonify
from server import app

import webbrowser

def getNewWindow(msg):
    html = '<!DOCTYPE html><html><body>'
    html += '<p>Click the button to open a new browser window.</p>'
    html += '<button onclick="myFunction()">More informaton</button>'
    html += '<script>'
    html += 'function myFunction(){'
    html += 'var w = window.open("", "MsgWindow", "width=320,height=180");'
    html += 'w.document.write("<p>' + str(msg) + ' </p>");'
    html += '}'
    html += '</script>'
    html += '</body></html>'
    return html

@app.route('/v1')
def index():
    return getV1Page()

@app.route('/v2')
def loginPage():
    html = '<!DOCTYPE html>\n<html>\n<body>\n'
    html += '<p>Click the button to open a new browser window.</p>'
    html += '<button onclick="myFunction()">Try it</button>'
    html += '<script>'
    html += 'function myFunction(){'
#    html += 'window.open("https://www.w3schools.com/jsref/met_win_open.asp");'
    html += 'var w = window.open("", "MsgWindow", "width=200,height=100");'
    html += 'w.document.write("<p>This is MsgWindow. I am 200px wide and 100px tall!</p>");' 
    html += '}'
    html += '</script>'
    html += '</body></html>'
#    return html
    # return jsonify("unter construction")
    return app.send_static_file('login.html')

@app.route('/v1/report/<string:name>', methods=['GET'])
@app.route('/v1/report/<string:name>/<int:beginTs>/<int:endTs>', methods=['GET'])
def getReport(name, beginTs=0, endTs=365):
    update = lastUpdate(name)
    return getFigure(name, update, beginTs, endTs)

@app.route('/v1/compare/<string:name>', methods=['GET'])
@app.route('/v1/compare/<string:name>/<int:beginTs>/<int:endTs>', methods=['GET'])
def compare(name, beginTs=0, endTs=365):
    update = lastUpdate(name)
    queryDict = {x:request.args[x] for x in request.args}
    queryJson = json.dumps(queryDict)
    msg, htmlFigure = getCompare(name, queryJson = queryJson, beginTs=0, endTs=365)
    strg = msg.replace(',', '</p><p>')
    htmlInfo = getNewWindow(strg)
    return htmlInfo + htmlFigure
#    return jsonify(msg)

@app.route('/v1/info/<string:name>', methods=['GET'])
@formater
def getInfoV1(name):
    update = lastUpdate(name)
    return getData(name, {'type':'fix'}, update)

@app.route('/v1/query/<string:name>', methods=['GET'])
@app.route('/v1/query/<string:name>/<int:beginTs>/<int:endTs>', methods=['GET'])
@formater
def getDataV1(name, beginTs=0, endTs=365):
    queryDict = {x:request.args[x] for x in request.args}
    update = lastUpdate(name)
    queryJson = json.dumps(queryDict)
    return getData(name, queryJson, update, beginTs, endTs)

@app.route('/v1/cache', methods=['GET'])
@formater
def getCacheInfo():
    s = dict()
    s["getFigure"] = getFigure.cache_info()
    s["getCompare"] = getCompare.cache_info()
    s["getData"] = getData.cache_info()
    df = DataFrame.from_dict(s, orient='index', columns=['hit', 'misses', 'maxsize', 'size'])
    #df['function'] = df.index
    df.reset_index(level=0,inplace=True)
    return df
