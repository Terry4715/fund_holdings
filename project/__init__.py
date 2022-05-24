#%%
import os
from dotenv import load_dotenv
from .database import Database, CursorFromPool
from flask import Flask, render_template
import json


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

# load asset type allocation using fund_assets view from database
with CursorFromPool() as cursor:
    cursor.execute('''SELECT SUM(weight) AS weight,
                          asset_type
                      FROM fund_assets
                      GROUP BY asset_type
                      ORDER BY weight DESC;''')
    # converts sql response from cursor object to a list of tuples
    fund_a_type = cursor.fetchall()
# converts list of tuples into a dictionary
fund_a_type_label = [data[1] for data in fund_a_type]
fund_a_type_data = [str(data[0]) for data in fund_a_type]
#%%

# load asset region allocation using fund_assets view from database
with CursorFromPool() as cursor:
    cursor.execute('''SELECT SUM(weight) as weight,
                          region
                      FROM fund_assets
                      GROUP BY region
                      ORDER BY weight DESC;''')
    # converts sql response from cursor object to a list of tuples
    fund_a_region = cursor.fetchall()
# converts list of tuples into a dictionary
fund_a_region_dict = dict((y, x) for x, y in fund_a_region)

# load equity sector allocation using fund_assets view from database
with CursorFromPool() as cursor:
    cursor.execute('''SELECT SUM(ROUND((notional /
                      (SELECT SUM(notional) from fund_assets
                       WHERE asset_type = 'Equity')), 4)) as weight,
                          sector
                      FROM fund_assets
                      WHERE asset_type = 'Equity'
                      GROUP BY sector
                      ORDER BY weight DESC;''')
    # converts sql response from cursor object to a list of tuples
    fund_a_sector = cursor.fetchall()
# converts list of tuples into a dictionary
fund_a_sector_dict = dict((y, x) for x, y in fund_a_sector)


app = Flask(__name__)


@app.route("/")
def home():
    return render_template("home.html",
                           fund_assets=fund_assets,
                           a_type_label=json.dumps(fund_a_type_label),
                           a_type_data=json.dumps(fund_a_type_data))


@app.errorhandler(404)
def page_not_found(error):
    return render_template("404.html"), 404
