-------------------------------------------------------------------------------------------------------------------------
/*

here i will create a row table into bronze layer with bulk insertion.
before insertion into table i am using truncate table to avoid double data insertion into table
also used stored procedure for bulk data load into tables  

*/
---------------------------------------------------------------------------------------------------------------------------
use datawarehouse;

-- Create CRM table now

if OBJECT_ID('bronze.crm_cust_info','U') IS NOT NULL
	drop table bronze.crm_cust_info;
Create table bronze.crm_cust_info(
		cst_id int,
		cst_key nvarchar(50),
		cst_firstname nvarchar(250),
		cst_lastname nvarchar(250),
		cst_marital_status nvarchar(250),
		cst_gndr nvarchar(50),
		cst_create_date Date
);
GO

if OBJECT_ID('bronze.crm_prd_info','U') IS NOT NULL
	drop table bronze.crm_prd_info;
CREATE TABLE bronze.crm_prd_info(
		prd_id INT,
		prd_key NVARCHAR(50),
		prd_nm NVARCHAR(250),
		prd_cost INT,
		prd_line VARCHAR(50),
		prd_start_dt DATE,
		prd_end_dt DATE
);
GO

if OBJECT_ID('bronze.crm_sales_details','U') IS NOT NULL
	drop table bronze.crm_sales_details;
		CREATE TABLE bronze.crm_sales_details(
	sls_ord_num NVARCHAR(100),
	sls_prd_key NVARCHAR(100),
	sls_cust_id INT,
	sls_order_dt INT,
	sls_ship_dt INT,
	sls_due_dt INT,
	sls_sales INT,
	sls_quantity INT,
	sls_price INT

);
GO

-- Create erp table now

if OBJECT_ID('bronze.erp_CUST_AZ12','U') IS NOT NULL
	drop table bronze.erp_CUST_AZ12;
CREATE TABLE bronze.erp_CUST_AZ12(
CID NVARCHAR(50),
BDATE DATE,
GEN NVARCHAR(50)

);
GO

if OBJECT_ID('bronze.erp_LOC_A101','U') IS NOT NULL
	drop table bronze.erp_LOC_A101;
CREATE TABLE bronze.erp_LOC_A101(
CID	NVARCHAR(50),
CNTRY NVARCHAR(50)

);
GO

if OBJECT_ID('bronze.erp_PX_CAT_G1V2','U') IS NOT NULL
	drop table bronze.erp_PX_CAT_G1V2;
CREATE TABLE bronze.erp_PX_CAT_G1V2(
	ID NVARCHAR(50),
	CAT NVARCHAR(250),
	SUBCAT NVARCHAR(250),
	MAINTENANCE NVARCHAR(250)

);
GO

-- INSERT BULK DATA INTO ALL TABLE

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN

truncate table bronze.crm_cust_info;
BULK INSERT bronze.crm_cust_info
from 'C:\Users\Akshay\OneDrive\Documents\SQL_Data_Analysis\DataWarehouse project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
with(
	FIRSTROW = 2,
	FIELDTERMINATOR = ',' ,
	TABLOCK

);


truncate table bronze.crm_prd_info;
BULK INSERT bronze.crm_prd_info
from 'C:\Users\Akshay\OneDrive\Documents\SQL_Data_Analysis\DataWarehouse project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
with(
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);


TRUNCATE TABLE bronze.crm_sales_details;
BULK INSERT bronze.crm_sales_details
from 'C:\Users\Akshay\OneDrive\Documents\SQL_Data_Analysis\DataWarehouse project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
with(
	firstrow = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);


TRUNCATE TABLE bronze.erp_CUST_AZ12;
BULK INSERT bronze.erp_CUST_AZ12
from 'C:\Users\Akshay\OneDrive\Documents\SQL_Data_Analysis\DataWarehouse project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
with(
	firstrow = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);


TRUNCATE TABLE bronze.erp_LOC_A101;
BULK INSERT bronze.erp_LOC_A101
from 'C:\Users\Akshay\OneDrive\Documents\SQL_Data_Analysis\DataWarehouse project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
with(
	firstrow = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);


TRUNCATE TABLE bronze.erp_PX_CAT_G1V2;
BULK INSERT bronze.erp_PX_CAT_G1V2
from 'C:\Users\Akshay\OneDrive\Documents\SQL_Data_Analysis\DataWarehouse project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
with(
	firstrow = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
)
END


EXEC bronze.load_bronze;
