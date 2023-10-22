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
import os
import psycopg2
from psycopg2.extensions import parse_dsn

"""
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
"""

@flask_app.route('/api')
@cross_origin()
def Welcome():
    return "Welcome to the API!!!"

"""
@flask_app.route('/users/<int:googleid>', methods = ['GET'])
@cross_origin()
def Get_User(googleid:int):
    conn, cursor = create_conn()
    token = request.args.get("token")
    admin = backend.tokens.Check_Token(googleid, token, cursor, conn)
    if(admin["loggedin"] == False):#Check if user is logged in
        return admin
    cursor.execute("SELECT * FROM UserTable WHERE googleid= '{}'" .format(googleid),)
    UserRowTuple = cursor.fetchone()
    UserRowJson = json.dumps(UserRowTuple)
    conn.close()
    return UserRowJson

@flask_app.route('/users', methods = ['PUT'])
@cross_origin()
def Put_User():#googleid:int, firstname:str, lastname:str, isadmin:str, accountcreated:str, lastlogin:str):
    conn, cursor = create_conn()
    cursor.execute()
    conn.close()
    return {}

#@flask_app.route(, methods = ['GET'])
#@cross_origin()
#def Get_RSVP(googleid:int):
#    conn, cursor = create_conn()
#    return UserRowJson

#@flask_app.route('/rsvp', methods = ['PUT'])
#@cross_origin()
#def Put_RSVP():
#    conn, cursor = create_conn()
#    conn.commit()
#    conn.close()
#    return {}

#@flask_app.route('/statuses', methods = ['GET'])
#@cross_origin()
#def Get_Statuses():
#    conn, cursor = create_conn()
#    conn.close()
#    return UserRowJson

#@flask_app.route('/statuses', methods = ['PUT'])
#@cross_origin()
#def Put_Statuses():
#    conn, cursor = create_conn()
#    conn.commit()
#    conn.close()
#    return {"status":200}

#@flask_app.route('/token', methods = ['GET'])
#@cross_origin()
#def Get_Token():
#    conn, cursor = create_conn()
#    conn.close()
#    return admin

#@flask_app.route('/users')
#@cross_origin()
#def Get_Users():
#    conn, cursor = create_conn()
#    conn.close()
#    return UserRowJson

#@flask_app.route('/rsvp')
#@cross_origin()
#def Get_RSVPs():
#    conn, cursor = create_conn()
#    conn.close()
#    return UserRowJson
"""
