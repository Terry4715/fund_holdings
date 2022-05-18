
import os
from dotenv import load_dotenv
from database import Database, CursorFromPool
import pandas as pd

# loading environment variables used to hide database credentials
load_dotenv()

db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_host = os.getenv("DB_HOST")
db_name = os.getenv("DB_NAME")

# "../other_files/"
# reads data from excel and creates dataframe
file_path = "C:\\Users\\asusr\\Documents\\Python\\SMA Project\\other_files\\"
file_name = "db_design.xlsx"

excel_data = pd.read_excel(f"{file_path}{file_name}",
                           sheet_name="upload_template",
                           header=1)


# login to the database
Database.initialise(user=db_user,
                    password=db_password,
                    host=db_host,
                    database=db_name)


# FUNDS database table --------------------------------------------------------
# load existing 'funds' table from database
with CursorFromPool() as cursor:
    cursor.execute("SELECT * FROM funds;")
    # converts sql response from cursor object to a list of tuples
    funds_table = cursor.fetchall()
    # converts list of tuples into dataframe
    funds_table = pd.DataFrame(funds_table, columns=['fund_id',
                                                     'fund_name',
                                                     'fund_isin',
                                                     'fund_ric'])

# creates a subset dataframe from excel_data dataframe
new_funds = excel_data[['fund_name', 'fund_isin', 'fund_ric']]
# title case columns referenced in other tables
new_funds['fund_name'] = new_funds['fund_name'].str.title()
# drops duplicate records based on 'fund_name' column
new_funds = new_funds.drop_duplicates(subset=['fund_name'])
# drops rows with 'fund_name' that already exist in 'funds' database table
new_funds = (new_funds[~new_funds['fund_name'].str.lower()
             .isin(funds_table['fund_name'].str.lower())])
# converts dataframe into numpy array then list of tuples - required for upload
new_funds = list(map(tuple, new_funds.to_numpy()))

# if statement ensures there are new funds to upload
if len(new_funds):
    # uploads new funds into 'funds' database table
    with CursorFromPool() as cursor:
        sql_values = new_funds
        sql_params = ','.join(['%s'] * len(sql_values))
        sql_insert = f'''INSERT INTO funds (fund_name, fund_isin, fund_ric)
                         VALUES {sql_params};'''
        cursor.execute(sql_insert, sql_values)


# FUND_NAV database table -----------------------------------------------------
# load existing 'fund_nav' table from database
with CursorFromPool() as cursor:
    cursor.execute("SELECT * FROM fund_nav;")
    # converts sql response from cursor object to a list of tuples
    fund_nav_table = cursor.fetchall()
    # converts list of tuples into dataframe
    fund_nav_table = pd.DataFrame(fund_nav_table, columns=['fund_nav_id',
                                                           'fund_id',
                                                           'fund_nav_date',
                                                           'fund_nav'])
    # formats dataframe columns from 'object' to 'date' and 'float'
    fund_nav_table['fund_nav_date'] = (pd.to_datetime(
                                       fund_nav_table['fund_nav_date']))
    fund_nav_table['fund_nav'] = pd.to_numeric(fund_nav_table['fund_nav'])

# load newly updated 'funds' table from database --- 'fund_id' foreign key
with CursorFromPool() as cursor:
    cursor.execute("SELECT * FROM funds;")
    # converts sql response from cursor object to a list of tuples
    funds_table = cursor.fetchall()
    # converts list of tuples into dataframe
    funds_table = pd.DataFrame(funds_table, columns=['fund_id',
                                                     'fund_name',
                                                     'fund_isin',
                                                     'fund_ric'])

# creates a subset dataframe from excel_data dataframe
new_navs = excel_data[['date', 'fund_name', 'fund_nav']]
# title case columns referenced in other tables
new_navs['fund_name'] = new_navs['fund_name'].str.title()
# drops duplicate records based on 'fund_name' column
new_navs = new_navs.drop_duplicates(subset=['fund_name'])
# adds 'fund_id' by merging with 'funds' table on case insensitive 'fund_name'
new_navs = (pd.merge(funds_table['fund_id'], new_navs[['date', 'fund_nav']],
                     left_on=funds_table['fund_name'].str.lower(),
                     right_on=new_navs['fund_name'].str.lower(),
                     how='right'))
# drops rows with 'fund_id' and 'date' already in 'fund_nav' database table
new_navs = (new_navs[~new_navs.set_index(['fund_id', 'date']).index
            .isin(fund_nav_table
            .set_index(['fund_id', 'fund_nav_date']).index)]
            .iloc[:, 1:])  # drops key column created by merge
# converts dataframe into numpy array then list of tuples - required for upload
new_navs = list(map(tuple, new_navs.to_numpy()))

# if statement ensures there is fund_nav data to upload
if len(new_navs):
    # uploads new fund navs into 'fund_nav' database table
    with CursorFromPool() as cursor:
        sql_values = new_navs
        sql_params = ','.join(['%s'] * len(sql_values))
        sql_insert = f'''INSERT INTO fund_nav (fund_id, fund_nav_date, fund_nav)
                         VALUES {sql_params};'''
        cursor.execute(sql_insert, sql_values)


# REGION database table -------------------------------------------------------
# obtains unique countries from the excel_data dataframes 'country' column
countries = excel_data.country.unique()
# formats all countries to title case
countries = map(lambda x: x.title() if isinstance(x, str) else x, countries)
# converts countries object into a list of tuples - required for upload
countries = list(zip(countries))

# uploads countries into 'region' database table without creating duplicates
with CursorFromPool() as cursor:
    sql_values = countries
    sql_params = ','.join(['%s'] * len(sql_values))
    sql_insert = f'''INSERT INTO region (country)
                     VALUES {sql_params}
                     ON CONFLICT (country) DO NOTHING;'''
    cursor.execute(sql_insert, sql_values)


# ASSETS database table -------------------------------------------------------
# load existing 'assets' table from database
with CursorFromPool() as cursor:
    cursor.execute("SELECT * FROM assets;")
    # converts sql response from cursor object to a list of tuples
    assets_table = cursor.fetchall()
    # converts list of tuples into dataframe
    assets_table = pd.DataFrame(assets_table, columns=['asset_id',
                                                       'asset_name',
                                                       'asset_isin',
                                                       'asset_ric',
                                                       'asset_type',
                                                       'sector',
                                                       'country'])

# creates a subset dataframe from excel_data dataframe
new_assets = excel_data[['asset_name', 'asset_isin', 'asset_ric',
                         'asset_type', 'sector', 'country']]
# title case columns referenced in other tables
new_assets['asset_name'] = new_assets['asset_name'].str.title()
new_assets['asset_type'] = new_assets['asset_type'].str.title()
new_assets['sector'] = new_assets['sector'].str.title()
new_assets['country'] = new_assets['country'].str.title()
# drops duplicate records based on 'asset_name' column
new_assets = new_assets.drop_duplicates(subset=['asset_name'])
# drops rows with 'asset_name' that already exist in 'assets' database table
new_assets = (new_assets[~new_assets['asset_name'].str.lower()
              .isin(assets_table['asset_name'].str.lower())])
# converts dataframe into numpy array then list of tuples - required for upload
new_assets = list(map(tuple, new_assets.to_numpy()))

# if statement ensures there are new assets to upload
if len(new_assets):
    # uploads new assets into 'assets' database table
    with CursorFromPool() as cursor:
        sql_values = new_assets
        sql_params = ','.join(['%s'] * len(sql_values))
        sql_insert = f'''INSERT INTO assets (asset_name, asset_isin, asset_ric,
                                             asset_type, sector, country)
                         VALUES {sql_params};'''
        cursor.execute(sql_insert, sql_values)


# FUND_HOLDINGS database table ------------------------------------------------
# load existing 'fund_holdings' table from database
with CursorFromPool() as cursor:
    cursor.execute("SELECT * FROM fund_holdings;")
    # converts sql response from cursor object to a list of tuples
    fund_holdings_table = cursor.fetchall()
    # converts list of tuples into dataframe
    fund_holdings_table = pd.DataFrame(fund_holdings_table,
                                       columns=['fund_holding_id',
                                                'fund_id',
                                                'asset_id',
                                                'fund_holding_date',
                                                'fund_asset_weight'])
    # formats dataframe columns from 'object' to 'date' and 'float'
    fund_holdings_table['fund_holding_date'] = (pd.to_datetime(
                                                fund_holdings_table
                                                ['fund_holding_date']))
    fund_holdings_table['fund_asset_weight'] = (pd.to_numeric(
                                                fund_holdings_table
                                                ['fund_asset_weight']))

# updated 'funds' table already loaded from database --- 'fund_id' foreign key

# load newly updated 'assets' table from database --- 'asset_id' foreign key
with CursorFromPool() as cursor:
    cursor.execute("SELECT * FROM assets;")
    # converts sql response from cursor object to a list of tuples
    assets_table = cursor.fetchall()
    # converts list of tuples into dataframe
    assets_table = pd.DataFrame(assets_table, columns=['asset_id',
                                                       'asset_name',
                                                       'asset_isin',
                                                       'asset_ric',
                                                       'asset_type',
                                                       'sector',
                                                       'country'])

# new_navs --> new_holdings
# creates a subset dataframe from excel_data dataframe
new_holdings = excel_data[['fund_name', 'asset_name',
                           'date', 'fund_asset_weight']]
# title case columns referenced in other tables
new_holdings['fund_name'] = new_holdings['fund_name'].str.title()
new_holdings['asset_name'] = new_holdings['asset_name'].str.title()
# adds 'asset_id' by merging with 'assets' table, case insensitive 'asset_name'
new_holdings = (pd.merge(assets_table['asset_id'],
                new_holdings[['fund_name', 'date', 'fund_asset_weight']],
                left_on=assets_table['asset_name'].str.lower(),
                right_on=new_holdings['asset_name'].str.lower(),
                how='right').iloc[:, 1:])  # drops key column created by merge
# adds 'fund_id' by merging with 'funds' table on case insensitive 'fund_name'
new_holdings = (pd.merge(funds_table['fund_id'],
                new_holdings[['asset_id', 'date', 'fund_asset_weight']],
                left_on=funds_table['fund_name'].str.lower(),
                right_on=new_holdings['fund_name'].str.lower(),
                how='right').iloc[:, 1:])  # drops key column created by merge
# drop rows with 'fund_id' and 'date' already in 'fund_holdings' database table
new_holdings = (new_holdings[~new_holdings.set_index(['fund_id', 'date']).index
                             .isin(fund_holdings_table
                             .set_index(['fund_id', 'fund_holding_date'])
                             .index)])
# converts dataframe into numpy array then list of tuples - required for upload
new_holdings = list(map(tuple, new_holdings.to_numpy()))

# if statement ensures there is fund holdings data to upload
if len(new_holdings):
    # uploads new fund holdings into 'fund_holdings' database table
    with CursorFromPool() as cursor:
        sql_values = new_holdings
        sql_params = ','.join(['%s'] * len(sql_values))
        sql_insert = f'''INSERT INTO fund_holdings (fund_id,
                                                    asset_id,
                                                    fund_holding_date,
                                                    fund_asset_weight)
                         VALUES {sql_params};'''
        cursor.execute(sql_insert, sql_values)


# ASSET_ATTRIBUTES database table ---------------------------------------------
# load existing 'asset_attributes' table from database
with CursorFromPool() as cursor:
    cursor.execute("SELECT * FROM asset_attributes;")
    # converts sql response from cursor object to a list of tuples
    asset_attributes_table = cursor.fetchall()
    # converts list of tuples into dataframe
    asset_attributes_table = pd.DataFrame(asset_attributes_table,
                                          columns=['asset_attribute_id',
                                                   'asset_id',
                                                   'asset_attribute_date',
                                                   'asset_esg_score',
                                                   'asset_esg_contro_score'])
    # formats dataframe columns from 'object' to 'date' and 'float'
    asset_attributes_table['asset_attribute_date'] = (pd.to_datetime(
                                                     asset_attributes_table
                                                     ['asset_attribute_date']))
    asset_attributes_table['asset_esg_score'] = (pd.to_numeric(
                                                 asset_attributes_table
                                                 ['asset_esg_score']))
    asset_attributes_table['asset_esg_contro_score'] = (pd.to_numeric(
                                                 asset_attributes_table
                                                 ['asset_esg_contro_score']))

# updated 'assets' table already loaded from database - 'asset_id' foreign key

# creates a subset dataframe from excel_data dataframe
new_aa = excel_data[['asset_name', 'date', 'asset_esg_score',
                     'asset_esg_contro_score']]
# title case columns referenced in other tables
new_aa['asset_name'] = new_aa['asset_name'].str.title()
# drops duplicate records based on 'asset_name' column
new_aa = new_aa.drop_duplicates(subset=['asset_name'])
# adds 'asset_id' by merging with 'assets' table, case insensitive 'asset_name'
new_aa = (pd.merge(assets_table['asset_id'],
                   new_aa[['date',
                           'asset_esg_score',
                           'asset_esg_contro_score']],
                   left_on=assets_table['asset_name'].str.lower(),
                   right_on=new_aa['asset_name'].str.lower(),
                   how='right')).iloc[:, 1:]  # drop column created by merge
# drops rows with 'asset_id' and 'date' already in 'asset_attributes' DB table
new_aa = (new_aa[~new_aa.set_index(['asset_id', 'date']).index
          .isin(asset_attributes_table
          .set_index(['asset_id', 'asset_attribute_date']).index)])
# converts dataframe into numpy array then list of tuples - required for upload
new_aa = list(map(tuple, new_aa.to_numpy()))

# if statement ensures there is asset attribution data to upload
if len(new_aa):
    # uploads new asset attributes into 'asset_attributes' database table
    with CursorFromPool() as cursor:
        sql_values = new_aa
        sql_params = ','.join(['%s'] * len(sql_values))
        sql_insert = f'''INSERT INTO asset_attributes (asset_id,
                                                       asset_attribute_date,
                                                       asset_esg_score,
                                                       asset_esg_contro_score)
                         VALUES {sql_params};'''
        cursor.execute(sql_insert, sql_values)
