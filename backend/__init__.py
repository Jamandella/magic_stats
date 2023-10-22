from flask import Flask
from flask_cors import CORS


flask_app = Flask(__name__, static_folder='../frontend/dist', static_url_path='/')
CORS(flask_app)
