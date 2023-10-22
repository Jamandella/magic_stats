#!/usr/bin/env python3
from flask import send_from_directory
from flask_cors import cross_origin
from backend.endpoints import flask_app


@flask_app.route('/')
@cross_origin()
def serve():
    return send_from_directory(flask_app.static_folder, 'index.html')


@flask_app.errorhandler(404)
@cross_origin()
def not_found(e):
    return send_from_directory(flask_app.static_folder, 'index.html')


if __name__ == '__main__':
    flask_app.run(host='0.0.0.0')
