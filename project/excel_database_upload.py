# #%%
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
# #%%

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

# load newly updated 'funds' table from database --- foreign key
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
# drops duplicate records based on 'fund_name' column
new_navs = new_navs.drop_duplicates(subset=['fund_name'])
# adds 'fund_id' by merging with 'funds' table on case insensitive 'fund_name'
new_navs = (pd.merge(funds_table['fund_id'], new_navs[['date', 'fund_nav']],
                     left_on=funds_table['fund_name'].str.lower(),
                     right_on=new_navs['fund_name'].str.lower(),
                     how='right'))

# keeps rows with 'fund_id' that already exist in 'fund_nav' database table
update_navs = (new_navs[new_navs.set_index(['fund_id', 'date']).index
               .isin(fund_nav_table
               .set_index(['fund_id', 'fund_nav_date']).index)]
               .iloc[:, 1:])  # drops key column created by merge

# converts dataframe into numpy array then list of tuples - required for upload
update_navs = list(map(tuple, update_navs.to_numpy()))

# drops rows with 'fund_id' that already exist in 'fund_nav' database table
insert_navs = (new_navs[~new_navs['fund_id']
               .isin(fund_nav_table['fund_id'])]
               .iloc[:, 1:])  # drops key column created by merge
# converts dataframe into numpy array then list of tuples - required for upload
insert_navs = list(map(tuple, insert_navs.to_numpy()))

# if statement ensures there is fund_nav data to upload
if len(update_navs):
    # updates fund navs within the 'fund_nav' database table
    with CursorFromPool() as cursor:
        sql_values = update_navs
        sql_params = ','.join(['%s'] * len(sql_values))
        sql_insert = f'''UPDATE fund_nav AS t
                         SET fund_nav_date = e.date,
                             fund_nav = e.fund_nav
                         FROM (VALUES {sql_params})
                         AS e(fund_id, date, fund_nav)
                         WHERE e.fund_id = t.fund_id;'''
        cursor.execute(sql_insert, sql_values)

# if statement ensures there is fund_nav data to upload
if len(insert_navs):
    # uploads new fund navs into 'fund_nav' database table
    with CursorFromPool() as cursor:
        sql_values = insert_navs
        sql_params = ','.join(['%s'] * len(sql_values))
        sql_insert = f'''INSERT INTO fund_nav (fund_id, fund_nav_date, fund_nav)
                         VALUES {sql_params};'''
        cursor.execute(sql_insert, sql_values)

# idea - build class so database tables can be called via methods - refine code
