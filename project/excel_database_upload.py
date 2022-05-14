
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


# REGION database table -------------------------------------------------------
# obtains unique countries from the dataframe 'country' column
countries = excel_data.country.unique()
# removes blank values from unique list of countries
countries = filter(lambda x: x == x, countries)
# formats all counties to title case
countries = map(lambda x: x.title(), countries)
# converts countries object into a list of tuples - required for upload
countries = list(zip(countries))

# uploads countries into 'region' database table without creating duplicates
with CursorFromPool() as cursor:
    print("Uploading data into database...")
    sql_values = countries
    sql_params = ','.join(['%s'] * len(sql_values))
    sql_insert = f'''INSERT INTO region (country)
                     VALUES {sql_params}
                     ON CONFLICT (country) DO NOTHING;'''
    cursor.execute(sql_insert, sql_values)


# FUNDS database table --------------------------------------------------------
# creates a subset dataframe used for the 'funds' database table
df_funds = excel_data[['fund_name', 'fund_isin', 'fund_ric']]
# drops duplicate records based on 'fund_name' column
df_funds = df_funds.drop_duplicates(subset=['fund_name'])
# converts dataframe into numpy array then list of tuples - required for upload
df_funds = list(map(tuple, df_funds.to_numpy()))

# uploads fund_names into 'funds' database table without creating duplicates
with CursorFromPool() as cursor:
    print("Uploading data into database...")
    sql_values = df_funds
    sql_params = ','.join(['%s'] * len(sql_values))
    sql_insert = f'''INSERT INTO funds (fund_name, fund_isin, fund_ric)
                     VALUES {sql_params}
                     ON CONFLICT (fund_name) DO NOTHING;'''
    cursor.execute(sql_insert, sql_values)

# this works but need to address issue with duplicate uploads, name indexed?

# comparator2_pricing_data_list = create_pricing_subset_list(comp2)


# load data from database
# with CursorFromPool() as cursor:
#     print("Loading data from database...")
#     cursor.execute("SELECT * FROM testing;")
#     for record in cursor:
#         print(record)
