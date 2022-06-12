import os
from dotenv import load_dotenv
from database import Database, CursorFromPool

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

# create funds table
with CursorFromPool() as cursor:
    cursor.execute('''CREATE TABLE IF NOT EXISTS funds (
                        fund_id	BIGSERIAL NOT NULL PRIMARY KEY,
                        fund_name VARCHAR(250) NOT NULL,
                        fund_ric VARCHAR(32),
                        fund_isin CHAR(12)  NOT NULL,
                        fund_type VARCHAR(12) NOT NULL
                        )''')

# create fund_nav table
with CursorFromPool() as cursor:
    cursor.execute('''CREATE TABLE IF NOT EXISTS fund_nav (
                        fund_nav_id	BIGSERIAL NOT NULL PRIMARY KEY,
                        fund_id	BIGINT NOT NULL,
                        fund_nav_date DATE NOT NULL,
                        fund_nav DECIMAL(14,2) NOT NULL,
                        FOREIGN KEY(fund_id) REFERENCES funds(fund_id)
                        )''')

# create region table
with CursorFromPool() as cursor:
    cursor.execute('''CREATE TABLE IF NOT EXISTS region (
                        country VARCHAR(250) NOT NULL PRIMARY KEY,
                        region VARCHAR(250)
                        )''')

# create assets table
with CursorFromPool() as cursor:
    cursor.execute('''CREATE TABLE IF NOT EXISTS assets (
                        asset_id BIGSERIAL NOT NULL PRIMARY KEY,
                        asset_name VARCHAR(250) NOT NULL,
                        asset_isin CHAR(12),
                        asset_ric VARCHAR(32),
                        asset_type VARCHAR(75),
                        sector VARCHAR(75),
                        country VARCHAR(250),
                        FOREIGN KEY(country) REFERENCES region(country)
                        )''')

# create fund_holdings table
with CursorFromPool() as cursor:
    cursor.execute('''CREATE TABLE IF NOT EXISTS fund_holdings (
                        fund_holding_id	BIGSERIAL NOT NULL PRIMARY KEY,
                        fund_id	BIGINT NOT NULL,
                        asset_id BIGINT NOT NULL,
                        fund_holding_date DATE NOT NULL,
                        fund_asset_weight DECIMAL(7,6) NOT NULL,
                        FOREIGN KEY(fund_id) REFERENCES funds(fund_id),
                        FOREIGN KEY(asset_id) REFERENCES assets(asset_id)
                        )''')

# create asset_attributes table
with CursorFromPool() as cursor:
    cursor.execute('''CREATE TABLE IF NOT EXISTS asset_attributes (
                        asset_attribute_id BIGSERIAL NOT NULL PRIMARY KEY,
                        asset_id BIGINT	NOT NULL,
                        asset_attribute_date DATE NOT NULL,
                        asset_esg_score DECIMAL(5,2),
                        asset_esg_contro_score DECIMAL(5,2),
                        FOREIGN KEY(asset_id) REFERENCES assets(asset_id)
                        )''')

print('''
         ---Database tables created---
      ''')
