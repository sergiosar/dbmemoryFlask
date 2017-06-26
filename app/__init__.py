from flask import Flask,jsonify, request
import threading
import time
import bisect


app = Flask(__name__)
from app import views