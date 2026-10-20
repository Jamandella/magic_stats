#!/usr/bin/env python3
from flask import send_from_directory
from backend import flask_app
from backend.endpoints import api_bp


# Register API Blueprint
flask_app.register_blueprint(api_bp)


@flask_app.route('/')
def serve():
    return send_from_directory(flask_app.static_folder, 'index.html')


@flask_app.errorhandler(404)
def not_found(e):
    return send_from_directory(flask_app.static_folder, 'index.html')


if __name__ == '__main__':
    flask_app.run(host='0.0.0.0', port=3000)
