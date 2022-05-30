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

# load fund holdings from database
with CursorFromPool() as cursor:
    cursor.execute('''SELECT asset_name,
                             fund_asset_weight,
                             ROUND(SUM(fund_asset_weight * fund_nav.fund_nav)
                                     ,0) AS notional,
                             asset_isin
                      FROM fund_holdings
                      INNER JOIN assets
                      ON assets.asset_id = fund_holdings.asset_id
                      INNER JOIN fund_nav
                      ON fund_nav.fund_id = fund_holdings.fund_id
                      WHERE fund_holdings.fund_id = 1
                      GROUP BY asset_name, fund_asset_weight, asset_isin
                      ORDER BY notional DESC;''')
    # converts sql response from cursor object to a list of tuples
    fund_holdings = cursor.fetchall()

# load asset type allocation using fund_assets view from database
with CursorFromPool() as cursor:
    cursor.execute('''SELECT SUM(weight) AS weight,
                          asset_type
                      FROM fund_assets
                      GROUP BY asset_type
                      ORDER BY weight DESC;''')
    # converts sql response from cursor object to a list of tuples
    fund_a_type = cursor.fetchall()
# converts list of tuples into ordered lists
fund_a_type_label = [data[1] for data in fund_a_type]
fund_a_type_data = [str(data[0]) for data in fund_a_type]

# load asset region allocation using fund_assets view from database
with CursorFromPool() as cursor:
    cursor.execute('''SELECT SUM(weight) as weight,
                          region
                      FROM fund_assets
                      GROUP BY region
                      ORDER BY weight DESC;''')
    # converts sql response from cursor object to a list of tuples
    fund_a_region = cursor.fetchall()
# converts list of tuples into ordered lists
fund_a_region_label = [data[1] for data in fund_a_region]
fund_a_region_data = [str(data[0]) for data in fund_a_region]

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
# converts list of tuples into ordered lists
fund_a_sector_label = [data[1] for data in fund_a_sector]
fund_a_sector_data = [str(data[0]) for data in fund_a_sector]


app = Flask(__name__)


@app.route("/")
def home():
    return render_template("home.html",
                           title="Fund Holdings Analysis",
                           fund_assets=fund_assets,
                           fund_holdings=fund_holdings,
                           a_type_label=json.dumps(fund_a_type_label),
                           a_type_data=json.dumps(fund_a_type_data),
                           a_region_label=json.dumps(fund_a_region_label),
                           a_region_data=json.dumps(fund_a_region_data),
                           a_sector_label=json.dumps(fund_a_sector_label),
                           a_sector_data=json.dumps(fund_a_sector_data))


@app.errorhandler(404)
def page_not_found(error):
    return render_template("404.html"), 404
