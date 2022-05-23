import os
from dotenv import load_dotenv
from .database import Database, CursorFromPool
from flask import Flask, render_template


# loading environment variables used to hide database credentials
load_dotenv()

db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_host = os.getenv("DB_HOST")
db_name = os.getenv("DB_NAME")

# login to the database
Database.initialise(user=db_user,
                    password=db_password,
                    host=db_host,
                    database=db_name)

# load fund_assets view from database
with CursorFromPool() as cursor:
    cursor.execute("SELECT * FROM fund_assets;")
    # converts sql response from cursor object to a list of tuples
    fund_assets = cursor.fetchall()


app = Flask(__name__)


def to_dict(self):
    return {
        'name': self.name,
        'weight': self.weight,
        'notional': self.notional,
        'type': self.type,
        'region': self.region,
        'sector': self.sector
        }


@app.route("/")
def home():
    return render_template("home.html", fund_assets=fund_assets)


@app.errorhandler(404)
def page_not_found(error):
    return render_template("404.html"), 404
