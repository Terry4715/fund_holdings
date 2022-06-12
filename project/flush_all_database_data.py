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

# delete all data tables from fund_holdings database
with CursorFromPool() as cursor:
    cursor.execute('''DROP TABLE IF EXISTS
                        fund_holdings,
                        fund_nav,
                        funds,
                        asset_attributes,
                        assets,
                        region''')

print("fund_holdings database has been purged!")
