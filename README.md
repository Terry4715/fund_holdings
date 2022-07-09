# Fund Holdings Project

This project was designed to create a simple look through of assets for investment funds with a 'fund of funds' allocation strategy. This just means that at a top level you have an investment fund which holds several other investment funds as its assets. When analysing these types of funds it can be difficult to understand what the overall exposure to asset types, regions and equity sectors are in aggregate. 

The application looks at all of the underlying fund assets and rolls up their value to present the overall exposures. It also makes it very easy to search for specific assets and understand their contribution to data points such as CO2 emmisions. 

The project uses a postgres database, python as the backend using Flask, HTML, CSS and Javascript as the front end to present the asset data.
Data can be uploaded into the database using the upload template for excel

Asset attributes (data points) can be built out futher by design. 

