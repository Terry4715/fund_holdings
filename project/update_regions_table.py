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
    cursor.execute('''
    UPDATE region SET region = 'Rest of World' WHERE country = 'Africa';
    UPDATE region SET region = 'Rest of World' WHERE country = 'Argentina';
    UPDATE region SET region = 'Asia Pacific' WHERE country = 'Australia';
    UPDATE region SET region = 'European' WHERE country = 'Belgium';
    UPDATE region SET region = 'Rest of World' WHERE country = 'Bermuda';
    UPDATE region SET region = 'Rest of World' WHERE country = 'Brazil';
    UPDATE region SET region = 'North American' WHERE country = 'Canada';
    UPDATE region SET region = 'Rest of World' WHERE country ='Cayman Islands';
    UPDATE region SET region = 'Asia Pacific' WHERE country = 'China';
    UPDATE region SET region = 'Rest of World' WHERE country = 'Cyprus';
    UPDATE region SET region = 'European' WHERE country = 'Denmark';
    UPDATE region SET region = 'European' WHERE country = 'Finland';
    UPDATE region SET region = 'European' WHERE country = 'France';
    UPDATE region SET region = 'European' WHERE country = 'Germany';
    UPDATE region SET region = 'Asia Pacific' WHERE country = 'Hong Kong';
    UPDATE region SET region = 'Asia Pacific' WHERE country = 'India';
    UPDATE region SET region = 'Asia Pacific' WHERE country = 'Indonesia';
    UPDATE region SET region = 'European' WHERE country = 'Ireland';
    UPDATE region SET region = 'Rest of World' WHERE country = 'Israel';
    UPDATE region SET region = 'European' WHERE country = 'Italy';
    UPDATE region SET region = 'Asia Pacific' WHERE country = 'Japan';
    UPDATE region SET region = 'Asia Pacific' WHERE country = 'Korea';
    UPDATE region SET region = 'Rest of World' WHERE country = 'Luxembourg';
    UPDATE region SET region = 'Rest of World' WHERE country = 'Mexico';
    UPDATE region SET region = 'European' WHERE country = 'Netherlands';
    UPDATE region SET region = 'Rest of World' WHERE country = 'Panama';
    UPDATE region SET region = 'European' WHERE country = 'Poland';
    UPDATE region SET region = 'Russian' WHERE country = 'Russian Federation';
    UPDATE region SET region = 'Asia Pacific' WHERE country = 'Singapore';
    UPDATE region SET region = 'European' WHERE country = 'Spain';
    UPDATE region SET region = 'European' WHERE country = 'Sweden';
    UPDATE region SET region = 'European' WHERE country = 'Switzerland';
    UPDATE region SET region = 'Asia Pacific' WHERE country = 'Taiwan';
    UPDATE region SET region = 'UK' WHERE country = 'United Kingdom';
    UPDATE region SET region = 'North American' WHERE country ='United States';
        ''')

print('''
         ---Region database table updated---
      ''')
