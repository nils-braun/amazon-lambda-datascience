from flask import Flask
from amazon_lambda_datascience.app import add_routes

app = Flask(__name__)
add_routes(app)