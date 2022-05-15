
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
# obtains unique countries from the excel_data dataframes 'country' column
countries = excel_data.country.unique()
# removes blank values from unique list of countries
countries = filter(lambda x: x == x, countries)
# formats all countries to title case
countries = map(lambda x: x.title(), countries)
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
# drops duplicate records based on 'fund_name' column
new_funds = new_funds.drop_duplicates(subset=['fund_name'])
# drops rows with 'fund_name' that already exist in 'funds' database table
new_funds = (new_funds[~new_funds['fund_name'].str.lower()
             .isin(funds_table['fund_name'].str.lower())])
# converts dataframe into numpy array then list of tuples - required for upload
new_funds = list(map(tuple, new_funds.to_numpy()))

# if statement ensures there are new funds to upload
if len(new_funds):
    # uploads new funds into 'funds' database table without creating duplicates
    with CursorFromPool() as cursor:
        sql_values = new_funds
        sql_params = ','.join(['%s'] * len(sql_values))
        sql_insert = f'''INSERT INTO funds (fund_name, fund_isin, fund_ric)
                         VALUES {sql_params};'''
        cursor.execute(sql_insert, sql_values)
