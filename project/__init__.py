import os
from dotenv import load_dotenv
from .database import Database, CursorFromPool
from flask import Flask, render_template, request
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


app = Flask(__name__)


@app.route("/")
def home():

    # choosing fund, if no FID query parameter provided, default is FID=1
    if request.args:
        FID = request.args['FID']
        # default fund id for SQL queries
    else:
        FID = 1

    # load asset type allocation using fund_assets view from database
    with CursorFromPool() as cursor:
        sql_values = [FID] * 2
        sql_insert = '''WITH RECURSIVE fund_assets AS (
            SELECT
                assets.asset_name,
                assets.asset_isin,
                fund_holdings.fund_asset_weight,
                fund_nav.fund_nav,
                assets.asset_type,
                region.region,
                assets.sector
            FROM assets
            INNER JOIN fund_holdings ON fund_holdings.asset_id=assets.asset_id
            INNER JOIN fund_nav ON fund_nav.fund_id = fund_holdings.fund_id
            INNER JOIN region ON region.country = assets.country
            INNER JOIN funds ON funds.fund_id = fund_holdings.fund_id
            WHERE fund_holdings.fund_id = %s
            UNION ALL
            SELECT
                assets.asset_name,
                assets.asset_isin,
                fund_holdings.fund_asset_weight,
                fund_nav.fund_nav,
                assets.asset_type,
                region.region,
                assets.sector
            FROM assets
            INNER JOIN fund_holdings ON fund_holdings.asset_id=assets.asset_id
            INNER JOIN fund_nav ON fund_nav.fund_id = fund_holdings.fund_id
            INNER JOIN region ON region.country = assets.country
            INNER JOIN funds ON funds.fund_id = fund_holdings.fund_id
            INNER JOIN fund_assets ON fund_assets.asset_isin = funds.fund_isin)
        SELECT
            ROUND(SUM(fund_asset_weight * fund_nav) /
            (SELECT fund_nav FROM fund_nav WHERE fund_id = %s),4) AS weight,
            asset_type
        FROM fund_assets
        WHERE asset_type != 'Fund'
        GROUP BY asset_type
        ORDER BY weight DESC;'''
        cursor.execute(sql_insert, sql_values)
        # converts sql response from cursor object to a list of tuples
        fund_a_type = cursor.fetchall()
    # converts list of tuples into ordered lists
    fund_a_type_label = [data[1] for data in fund_a_type]
    fund_a_type_data = [str(data[0]) for data in fund_a_type]

    # load asset region allocation using fund_assets view from database
    with CursorFromPool() as cursor:
        sql_values = [FID] * 2
        sql_insert = '''WITH RECURSIVE fund_assets AS (
            SELECT
                assets.asset_name,
                assets.asset_isin,
                fund_holdings.fund_asset_weight,
                fund_nav.fund_nav,
                assets.asset_type,
                region.region,
                assets.sector
            FROM assets
            INNER JOIN fund_holdings ON fund_holdings.asset_id=assets.asset_id
            INNER JOIN fund_nav ON fund_nav.fund_id = fund_holdings.fund_id
            INNER JOIN region ON region.country = assets.country
            INNER JOIN funds ON funds.fund_id = fund_holdings.fund_id
            WHERE fund_holdings.fund_id = %s
            UNION ALL
            SELECT
                assets.asset_name,
                assets.asset_isin,
                fund_holdings.fund_asset_weight,
                fund_nav.fund_nav,
                assets.asset_type,
                region.region,
                assets.sector
            FROM assets
            INNER JOIN fund_holdings ON fund_holdings.asset_id=assets.asset_id
            INNER JOIN fund_nav ON fund_nav.fund_id = fund_holdings.fund_id
            INNER JOIN region ON region.country = assets.country
            INNER JOIN funds ON funds.fund_id = fund_holdings.fund_id
            INNER JOIN fund_assets ON fund_assets.asset_isin = funds.fund_isin)
        SELECT
            ROUND(SUM(fund_asset_weight * fund_nav) /
            (SELECT fund_nav FROM fund_nav WHERE fund_id = %s),4) AS weight,
            region
        FROM fund_assets
        WHERE asset_type != 'Fund'
        GROUP BY region
        ORDER BY weight DESC;'''
        cursor.execute(sql_insert, sql_values)
        # converts sql response from cursor object to a list of tuples
        fund_a_region = cursor.fetchall()
    # converts list of tuples into ordered lists
    fund_a_region_label = [data[1] for data in fund_a_region]
    fund_a_region_data = [str(data[0]) for data in fund_a_region]

    # load equity sector allocation using fund_assets view from database
    with CursorFromPool() as cursor:
        sql_values = [FID] * 1
        sql_insert = '''WITH RECURSIVE fund_assets AS (
            SELECT
                assets.asset_name,
                assets.asset_isin,
                fund_holdings.fund_asset_weight,
                fund_nav.fund_nav,
                assets.asset_type,
                region.region,
                assets.sector
            FROM assets
            INNER JOIN fund_holdings ON fund_holdings.asset_id=assets.asset_id
            INNER JOIN fund_nav ON fund_nav.fund_id = fund_holdings.fund_id
            INNER JOIN region ON region.country = assets.country
            INNER JOIN funds ON funds.fund_id = fund_holdings.fund_id
            WHERE fund_holdings.fund_id = %s
            UNION ALL
            SELECT
                assets.asset_name,
                assets.asset_isin,
                fund_holdings.fund_asset_weight,
                fund_nav.fund_nav,
                assets.asset_type,
                region.region,
                assets.sector
            FROM assets
            INNER JOIN fund_holdings ON fund_holdings.asset_id=assets.asset_id
            INNER JOIN fund_nav ON fund_nav.fund_id = fund_holdings.fund_id
            INNER JOIN region ON region.country = assets.country
            INNER JOIN funds ON funds.fund_id = fund_holdings.fund_id
            INNER JOIN fund_assets ON fund_assets.asset_isin = funds.fund_isin)
        SELECT
            SUM(ROUND((fund_asset_weight * fund_nav) /
                  (SELECT SUM(fund_asset_weight * fund_nav)
                FROM fund_assets WHERE asset_type = 'Equity'), 4)) as weight,
            sector
        FROM fund_assets
        WHERE asset_type = 'Equity'
        GROUP BY sector
        ORDER BY weight DESC;'''
        cursor.execute(sql_insert, sql_values)
        # converts sql response from cursor object to a list of tuples
        fund_a_sector = cursor.fetchall()
    # converts list of tuples into ordered lists
    fund_a_sector_label = [data[1] for data in fund_a_sector]
    fund_a_sector_data = [str(data[0]) for data in fund_a_sector]

    # load fund assets from database based in FID
    with CursorFromPool() as cursor:
        sql_values = [FID] * 2
        sql_insert = '''WITH RECURSIVE fund_assets AS (
            SELECT
                assets.asset_name,
                assets.asset_isin,
                fund_holdings.fund_asset_weight,
                fund_nav.fund_nav,
                assets.asset_type,
                region.region,
                assets.sector
            FROM assets
            INNER JOIN fund_holdings ON fund_holdings.asset_id=assets.asset_id
            INNER JOIN fund_nav ON fund_nav.fund_id = fund_holdings.fund_id
            INNER JOIN region ON region.country = assets.country
            INNER JOIN funds ON funds.fund_id = fund_holdings.fund_id
            WHERE fund_holdings.fund_id = %s
            UNION ALL
            SELECT
                assets.asset_name,
                assets.asset_isin,
                fund_holdings.fund_asset_weight,
                fund_nav.fund_nav,
                assets.asset_type,
                region.region,
                assets.sector
            FROM assets
            INNER JOIN fund_holdings ON fund_holdings.asset_id=assets.asset_id
            INNER JOIN fund_nav ON fund_nav.fund_id = fund_holdings.fund_id
            INNER JOIN region ON region.country = assets.country
            INNER JOIN funds ON funds.fund_id = fund_holdings.fund_id
            INNER JOIN fund_assets ON fund_assets.asset_isin = funds.fund_isin)
        SELECT
            asset_name,
            ROUND(SUM(fund_asset_weight * fund_nav) /
            (SELECT fund_nav FROM fund_nav WHERE fund_id = %s),4) AS weight,
            ROUND(SUM(fund_asset_weight * fund_nav),0) AS notional,
            asset_type,
            region,
            sector
        FROM fund_assets
        WHERE asset_type != 'Fund'
        GROUP BY asset_name, asset_type, region, sector
        ORDER BY notional DESC;'''
        cursor.execute(sql_insert, sql_values)
        # converts sql response from cursor object to a list of tuples
        fund_assets = cursor.fetchall()

    return render_template("home.html",
                           title="Fund Holdings Analysis",
                           fund_assets=fund_assets,
                           #    fund_holdings=fund_holdings,
                           a_type_label=json.dumps(fund_a_type_label),
                           a_type_data=json.dumps(fund_a_type_data),
                           a_region_label=json.dumps(fund_a_region_label),
                           a_region_data=json.dumps(fund_a_region_data),
                           a_sector_label=json.dumps(fund_a_sector_label),
                           a_sector_data=json.dumps(fund_a_sector_data))


@app.route("/search")
def search():

    # load list of funds from database
    with CursorFromPool() as cursor:
        cursor.execute("SELECT fund_id, fund_name FROM funds;")
        # converts sql response from cursor object to a list of tuples
        funds = cursor.fetchall()
        # converts list of tuples into dictionary
        funds = dict(funds)

    return render_template("search.html",
                           title="Fund Search", funds=funds)


@app.errorhandler(404)
def page_not_found(error):
    return render_template("404.html"), 404
