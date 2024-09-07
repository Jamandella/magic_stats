from flask import (
    Flask,
    render_template,
    send_from_directory,
    request,
    jsonify,
    json,
    make_response,
)
from flask_cors import cross_origin
from flask import Response
from . import flask_app
import backend.unit_test
import os
import psycopg2
import backend.stataccess
from psycopg2.extensions import parse_dsn

temp = os.environ['DATABASE_URL']
db_environ = parse_dsn(temp)

def create_conn():
    conn = psycopg2.connect(
        database = db_environ["dbname"],
        host = db_environ["host"],
        user = db_environ["user"],
        password = db_environ["password"],
        port = db_environ["port"]
    )
    cursor = conn.cursor()
    return conn, cursor


@flask_app.route('/api')
@cross_origin()
def Welcome():
    return "Welcome to the API!!!"


@flask_app.route('/cardInfo/<set_abbr>', methods = ['GET'])
@cross_origin()
def CardInfo(set_abbr:str):
    http_code = backend.unit_test.str_check(set_abbr)
    if(http_code != 200):
        return http_code
    else:
        json_card = backend.stataccess.cardInfo(set_abbr)
        return json_card


@flask_app.route('/getCardsWithColor/<color>/<set_abbr>/<include_multicolor>/<include_lands>/<as_string>', methods = ['GET'])
@cross_origin()
def CardsWithColor(color:str, set_abbr:str, include_multicolor:bool, include_lands:bool, as_string:bool):
    json_card = backend.stataccess.getCardsWithColor(color, set_abbr, include_multicolor, include_lands_as_string)
    http_code[0] = backend.unit_test.str_check(color)#unit test string
    http_code[1] = backend.unit_test.str_check(set_abbr)#unit test string
    http_code[2] = backend.unit_test.bool_check(include_multicolor)#unit test bool
    http_code[3] = backend.unit_test.bool_check(include_lands)#unit test bool
    http_code[4] = backend.unit_test.bool_check(as_string)#unit test bool
    for i in range (5):
        if(http_code[i] != 200):
            return http_code[i]#if a unit test fails return the http code
    return json_card


@flask_app.route('/getArchAvgCurve/<archLabel>/<set_abbr>', methods = ['GET'])
@cross_origin()
def ArchAvgCurve(archLabel:str, set_abbr:str):
    http_code[0] = backend.unit_test.str_check(archLabel)#unit test string
    http_code[1] = backend.unit_test.str_check(set_abbr)#unit test string
    for j in range (2):
        if(http_code[j] != 200):
            return http_code[j]#if a unit test fails return the http code
    json_card = backend.stataccess.getArchAvgCurve(archLabel, set_abbr)
    return json_card


@flask_app.route('/MTGsets', methods = ['GET']) #get a complete list of the MTG sets that are draftable and their 3 letter codes in a json
@cross_origin()
def Get_MTGsets():
    conn, cursor = create_conn() #set the cursor to the database
    cursor.execute("SELECT * FROM mtgSets") #select all the MTS sets in Full set name, 3 letter code as the columns
    MTGsets = cursor.fetchall() #set them equal to a local variable
    MTGsets_json = json.dumps(MTGsets)
    conn.close()
#    return MTGsets_json
    return {"response":MTGsets} #return the json


@flask_app.route('/getArchRecords/<archLabel>, <set_abbr>', methods = ['GET'])
@cross_origin()
def ArchRecords(archLabel:str, set_abbr:str):
    http_code[0] = backend.unit_test.str_check(archLabel)#unit test string
    http_code[1] = backend.unit_test.str_check(set_abbr)#unit test string
    for k in range(2):
        if(http_code[k] != 200):
            return http_code[k]#if a unit test fails return the http code
    json_card = backend.stataccess.getArchRecords(archLabel, set_abbr)
    return json_card


@flask_app.route('/getCardInDeckWinRates/<archLabels>/<minCopies>/<maxCopies>/<set_abbr>/<index_by_name>/<as_json>', methods = ['GET'])
@cross_origin()
def CardInDeckWinRates(archLabels:str, minCopies:int, maxCopies:int, set_abbr:str, index_by_name:str, as_json:bool) -> dict:
    http_code[0] = backend.unit_test.str_check(archLabels)#unit test string
    http_code[1] = backend.unit_test.int_check(minCopies)#unit test integer
    http_code[2] = backend.unit_test.int_check(maxCopies)#unit test integer
    http_code[3] = backend.unit_test.str_check(set_abbr)#unit test string
    http_code[4] = backend.unit_test.str_check(index_by_name)#unit test string
    http_code[5] = backend.unit_test.bool_check(as_json)#unit test bool
    for l in range(6):
        if(http_code[l] != 200):
            return http_code[l]#if a unit test fails return the http code
    json_card = getCardInDeckWinRates(archLabels, minCopies, maxCopies, set_abbr, index_by_name, as_json)
    return json_card


@flask_app.route('/get', methods = ['GET'])
@cross_origin()
def Get_Token():
    conn, cursor = create_conn()
    conn.close()
    return admin
