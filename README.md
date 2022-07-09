# Fund Holdings Project

This project was designed to create a simple look through of assets for investment funds with a 'fund of funds' allocation strategy. This just means that at a top level you have an investment fund which holds several other investment funds as its assets. When analysing these types of funds it can be difficult to understand the overall exposure to asset types, regions and equity sectors in aggregate. This application was built to make the analysis of fund holdings much easier.

The application looks at all of the underlying fund assets and rolls up their value to present the overall exposures. It also makes it very easy to search for specific assets and understand their contribution to data points such as CO2 emissions.

An example of this application has been deployed on render: https://fund-holdings-project.onrender.com/

Technology used includes:
- Python
- Flask
- PostgreSQL
- HTML
- CSS 
- JavaScript 

The database has been designed to normalise data uploaded from the excel 'upload_template.xlsx' workbook. Data quality controls should be implemented before uploading any new data into the database. 

Within the 'database' folder you will find the 'upload_template.xlsx' workbook, python scripts to create, upload and purge the fund_holdings database and there is a process note created to walk you through uploading data using the template provided. For those of you interest in the database design, here is a crude diagram below:

![Database design](https://github.com/Terry4715/fund_holdings/raw/master/database/database_design.PNG)

Funds typically publish their holdings data on a quarterly basis so the data snapshots for upload have been designed to be quarterly. The application allows you to view allocations through time where the data is available and also allows you to click through into specific funds using the fund search page.

If you have any questions, use my username to drop me an email @gmail.com
