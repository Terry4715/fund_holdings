import os
from dotenv import load_dotenv
from database import Database, CursorFromPool
from flask import Flask, render_template, request
import json
import datetime
from math import ceil


# loading environment variables used to hide database credentials
load_dotenv()

db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_host = os.getenv("DB_HOST")
db_name = os.getenv("DB_NAME")

app = Flask(__name__)

# login to the database
Database.initialise(user=db_user,
                    password=db_password,
                    host=db_host,
                    database=db_name)


@app.route("/")
def home():

    # choosing fund, if no FID query parameter provided, default is FID=1
    if request.args:
        FID = request.args['FID']
        fund_type = request.args['fund_type']
        # default fund id for SQL queries
    else:
        FID = 1
        fund_type = 'Blended'

    try:
        # load available fund holdings dates from database based on FID
        with CursorFromPool() as cursor:
            sql_values = [FID] * 1
            sql_insert = '''SELECT DISTINCT fund_holding_date
                            FROM fund_holdings
                            WHERE fund_id = %s
                            ORDER BY fund_holding_date DESC;'''
            cursor.execute(sql_insert, sql_values)
            # converts sql response from cursor object to a list of tuples
            sql_dates = cursor.fetchall()
    except OperationalError as e:
        print(e)
    else:
        # load available fund holdings dates from database based on FID
        with CursorFromPool() as cursor:
            sql_values = [FID] * 1
            sql_insert = '''SELECT DISTINCT fund_holding_date
                            FROM fund_holdings
                            WHERE fund_id = %s
                            ORDER BY fund_holding_date DESC;'''
            cursor.execute(sql_insert, sql_values)
            # converts sql response from cursor object to a list of tuples
            sql_dates = cursor.fetchall()

    # creates a list of tuples with formatted dates
    ddates = []
    for date in sql_dates:
        ddates.append((f"Q{ceil(date[0].month/3)} {date[0].year}",
                      str(date[0])))

    display_date = 'QX 20XX'

    # creates a default fund holdings date for SQL queries
    if request.args.get('date'):
        holdings_date = datetime.date.fromisoformat(request.args['date'])
        for d in ddates:
            if request.args['date'] in d:
                display_date = d[0]
    else:
        holdings_date = datetime.date.fromisoformat(ddates[0][1])
        display_date = ddates[0][0]

    # load fund info - name & nav, from database based on FID
    with CursorFromPool() as cursor:
        sql_values = [FID, holdings_date]
        sql_insert = '''SELECT
                            funds.fund_name,
                            fund_nav.fund_nav
                        FROM funds
                        INNER JOIN fund_nav ON
                        fund_nav.fund_id = funds.fund_id
                        WHERE funds.fund_id = %s AND fund_nav_date = %s;'''
        cursor.execute(sql_insert, sql_values)
        # converts sql response from cursor object to a list of tuples
        fund_info = cursor.fetchall()

        # error handling - build URL redirect function for better UX
        if fund_info:
            fund_name = fund_info[0][0]
            fund_nav = "£{:,.0f}".format(fund_info[0][1])
        else:
            fund_name = "No available data"
            fund_nav = "£-"

    if fund_type == 'Blended':
        # load blended fund holdings from database based on FID
        with CursorFromPool() as cursor:
            sql_values = [FID, holdings_date, holdings_date]
            sql_insert = '''SELECT
                                funds.fund_id,
                                funds.fund_type,
                                assets.asset_name,
                                fund_holdings.fund_asset_weight AS weight,
                                ROUND(SUM(fund_holdings.fund_asset_weight *
                                fund_nav.fund_nav),0) AS notional,
                                assets.asset_type
                            FROM assets
                            INNER JOIN fund_holdings ON fund_holdings.asset_id
                            = assets.asset_id
                            INNER JOIN fund_nav ON fund_nav.fund_id =
                            fund_holdings.fund_id
                            LEFT JOIN funds ON assets.asset_isin =
                            funds.fund_isin
                            WHERE fund_holdings.fund_id = %s
                                  AND fund_holding_date = %s
                                  AND fund_nav.fund_nav_date = %s
                            GROUP BY funds.fund_id, funds.fund_type,
                                assets.asset_name, weight, assets.asset_type
                            ORDER BY weight DESC;'''
            cursor.execute(sql_insert, sql_values)
            # converts sql response from cursor object to a list of tuples
            fund_holdings = cursor.fetchall()
    else:
        # arguement required to prevent error as fund_holdings is rendered
        fund_holdings = []

    # load asset type allocation using from database
    with CursorFromPool() as cursor:
        sql_values = [FID, holdings_date, holdings_date,
                      holdings_date, holdings_date, FID, holdings_date]
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
            WHERE fund_holdings.fund_id = %s AND
                  fund_holdings.fund_holding_date = %s AND
                  fund_nav.fund_nav_date = %s
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
            INNER JOIN fund_assets ON fund_assets.asset_isin = funds.fund_isin
            WHERE fund_holdings.fund_holding_date = %s AND
                  fund_nav.fund_nav_date = %s)
        SELECT
            ROUND(SUM(fund_asset_weight * fund_nav) /
            (SELECT fund_nav FROM fund_nav WHERE fund_id = %s
                    AND fund_nav_date = %s),4) AS weight,
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

    # load asset region allocation from database
    with CursorFromPool() as cursor:
        sql_values = [FID, holdings_date, holdings_date,
                      holdings_date, holdings_date, FID, holdings_date]
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
            WHERE fund_holdings.fund_id = %s AND
                  fund_holdings.fund_holding_date = %s AND
                  fund_nav.fund_nav_date = %s
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
            INNER JOIN fund_assets ON fund_assets.asset_isin = funds.fund_isin
            WHERE fund_holdings.fund_holding_date = %s AND
                  fund_nav.fund_nav_date = %s)
        SELECT
            ROUND(SUM(fund_asset_weight * fund_nav) /
            (SELECT fund_nav FROM fund_nav WHERE fund_id = %s
                    AND fund_nav_date = %s),4) AS weight,
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

    # load equity sector allocation from database
    with CursorFromPool() as cursor:
        sql_values = [FID, holdings_date, holdings_date,
                      holdings_date, holdings_date]
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
            WHERE fund_holdings.fund_id = %s AND
                  fund_holdings.fund_holding_date = %s AND
                  fund_nav.fund_nav_date = %s
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
            INNER JOIN fund_assets ON fund_assets.asset_isin = funds.fund_isin
            WHERE fund_holdings.fund_holding_date = %s AND
                  fund_nav.fund_nav_date = %s)
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

    # load fund assets from database based on FID
    with CursorFromPool() as cursor:
        sql_values = [FID, holdings_date, holdings_date, holdings_date,
                      holdings_date, holdings_date, holdings_date, FID,
                      holdings_date]
        sql_insert = '''WITH RECURSIVE fund_assets AS (
            SELECT
                assets.asset_name,
                assets.asset_isin,
                fund_holdings.fund_asset_weight,
                fund_nav.fund_nav,
                assets.asset_type,
                region.region,
                assets.sector,
                asset_attributes.asset_esg_score,
                asset_attributes.asset_total_co2,
                asset_attributes.asset_co2_scope1,
                asset_attributes.asset_co2_scope2
            FROM assets
            INNER JOIN fund_holdings ON fund_holdings.asset_id=assets.asset_id
            INNER JOIN fund_nav ON fund_nav.fund_id = fund_holdings.fund_id
            INNER JOIN region ON region.country = assets.country
            INNER JOIN asset_attributes ON asset_attributes.asset_id
                       = assets.asset_id
            INNER JOIN funds ON funds.fund_id = fund_holdings.fund_id
            WHERE fund_holdings.fund_id = %s AND
                  fund_holdings.fund_holding_date = %s AND
                  fund_nav.fund_nav_date = %s AND
                  asset_attributes.asset_attribute_date = %s
            UNION ALL
            SELECT
                assets.asset_name,
                assets.asset_isin,
                fund_holdings.fund_asset_weight,
                fund_nav.fund_nav,
                assets.asset_type,
                region.region,
                assets.sector,
                asset_attributes.asset_esg_score,
                asset_attributes.asset_total_co2,
                asset_attributes.asset_co2_scope1,
                asset_attributes.asset_co2_scope2
            FROM assets
            INNER JOIN fund_holdings ON fund_holdings.asset_id=assets.asset_id
            INNER JOIN fund_nav ON fund_nav.fund_id = fund_holdings.fund_id
            INNER JOIN region ON region.country = assets.country
            INNER JOIN asset_attributes ON asset_attributes.asset_id
                       = assets.asset_id
            INNER JOIN funds ON funds.fund_id = fund_holdings.fund_id
            INNER JOIN fund_assets ON fund_assets.asset_isin = funds.fund_isin
            WHERE fund_holdings.fund_holding_date = %s AND
                  fund_nav.fund_nav_date = %s AND
                  asset_attributes.asset_attribute_date = %s)
        SELECT
            asset_name,
            ROUND(SUM(fund_asset_weight * fund_nav) /
            (SELECT fund_nav FROM fund_nav WHERE fund_id = %s
                    AND fund_nav_date = %s),4) AS weight,
            ROUND(SUM(fund_asset_weight * fund_nav),0) AS notional,
            asset_type,
            region,
            sector,
            asset_esg_score,
            asset_total_co2,
            asset_co2_scope1,
            asset_co2_scope2
        FROM fund_assets
        WHERE asset_type != 'Fund'
        GROUP BY asset_name, asset_type, region, sector, asset_esg_score,
                 asset_total_co2, asset_co2_scope1, asset_co2_scope2
        ORDER BY notional DESC;'''
        cursor.execute(sql_insert, sql_values)
        # converts sql response from cursor object to a list of tuples
        fund_assets = cursor.fetchall()

    return render_template("home.html",
                           title="Fund Holdings Analysis",
                           FID=FID,
                           fund_type=fund_type,
                           fund_name=fund_name,
                           fund_nav=fund_nav,
                           display_date=display_date,
                           ddates=ddates,
                           fund_assets=fund_assets,
                           fund_holdings=fund_holdings,
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
        cursor.execute("SELECT fund_id, fund_name, fund_type FROM funds;")
        # converts sql response from cursor object to a list of tuples
        funds = cursor.fetchall()
        # converts list of tuples into dictionary
        # funds = dict(funds)

    return render_template("search.html",
                           title="Fund Search", funds=funds)


@app.errorhandler(404)
def page_not_found(error):
    return render_template("404.html"), 404


if __name__ == "__main__":
    app.run()

#%%