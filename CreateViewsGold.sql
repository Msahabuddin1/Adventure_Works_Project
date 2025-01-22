
------------------------------
-----CREATE VIEW CALENDAR-----
------------------------------
CREATE VIEW gold.calendar 
AS
SELECT *
FROM 
    OPENROWSET(
    BULK 'https://awpdeltalake.dfs.core.windows.net/silver/AdventureWorks_Calendar/',
    FORMAT = 'PARQUET'
    ) as Query1;



------------------------------
-----CREATE VIEW CUSTOMERS----
------------------------------
CREATE VIEW gold.customers 
AS
SELECT *
FROM 
    OPENROWSET(
    BULK 'https://awpdeltalake.dfs.core.windows.net/silver/AdventureWorks_Customers/',
    FORMAT = 'PARQUET'
    ) as Query1;



----------------------------------------
-----CREATE VIEW PRODUCT CATEGORIES-----
----------------------------------------
CREATE VIEW gold.productcat 
AS
SELECT *
FROM 
    OPENROWSET(
    BULK 'https://awpdeltalake.dfs.core.windows.net/silver/AdventureWorks_Product_Categories/',
    FORMAT = 'PARQUET'
    ) as Query1;



------------------------------
-----CREATE VIEW PRODUCTS-----
------------------------------
CREATE VIEW gold.product 
AS
SELECT *
FROM 
    OPENROWSET(
    BULK 'https://awpdeltalake.dfs.core.windows.net/silver/AdventureWorks_Products/',
    FORMAT = 'PARQUET'
    ) as Query1;



------------------------------
-----CREATE VIEW RETURNS------
------------------------------
CREATE VIEW gold.proreturns
AS
SELECT *
FROM 
    OPENROWSET(
    BULK 'https://awpdeltalake.dfs.core.windows.net/silver/AdventureWorks_Returns/',
    FORMAT = 'PARQUET'
    ) as Query1;



------------------------------
-------CREATE VIEW SALES------
------------------------------
CREATE VIEW gold.sales 
AS
SELECT *
FROM 
    OPENROWSET(
    BULK 'https://awpdeltalake.dfs.core.windows.net/silver/AdventureWorks_Sales/',
    FORMAT = 'PARQUET'
    ) as Query1;



---------------------------------
-----CREATE VIEW TERRITORIES-----
---------------------------------
CREATE VIEW gold.territories 
AS
SELECT *
FROM 
    OPENROWSET(
    BULK 'https://awpdeltalake.dfs.core.windows.net/silver/AdventureWorks_Territories/',
    FORMAT = 'PARQUET'
    ) as Query1;



------------------------------
-----CREATE VIEW SUBCATAG-----
------------------------------
CREATE VIEW gold.productsubcat 
AS
SELECT *
FROM 
    OPENROWSET(
    BULK 'https://awpdeltalake.dfs.core.windows.net/silver/Product_Subcategories/',
    FORMAT = 'PARQUET'
    ) as Query1;