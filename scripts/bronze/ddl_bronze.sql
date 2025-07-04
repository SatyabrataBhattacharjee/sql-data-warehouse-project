/*
--------------------------------------------------------------------------------------
    Script: ddl_bronze.sql
    Description:
        This script is part of the Data Warehouse ETL pipeline for initializing the
        Bronze Layer. It performs the following tasks:

        1. Drops existing Bronze tables if they already exist.
        2. Creates clean, schema-defined tables to store raw CRM and ERP data.
        3. These tables act as the landing zone for data ingested from various source
           systems such as sales, product, customer, and location datasets.
        
        The Bronze Layer maintains the data in its rawest usable form to ensure
        traceability and serve as the foundation for transformations into the Silver
        and Gold layers.
        
        Tables created in this script:
            - bronze.crm_sales_details
            - bronze.crm_prd_info
            - bronze.crm_cust_info
            - bronze.erp_cust_az12
            - bronze.erp_loc_a101
            - bronze.erp_px_cat_g1v2

    Usage:
        Run this script before loading data using the bronze.load_bronze procedure.
--------------------------------------------------------------------------------------
*/


IF OBJECT_ID ('bronze.crm_sales_details','U')  IS NOT NULL
	DROP TABLE bronze.crm_sales_details;
GO
CREATE TABLE bronze.crm_sales_details(
	sls_ord_num NVARCHAR(50),
	sls_prd_key NVARCHAR(50),
	sls_cust_id INT,
	sls_order_dt INT,
	sls_ship_dt INT,
	sls_due_dt INT,
	sls_sales INT,
	sls_quantity INT,
	sls_price INT
	);
IF OBJECT_ID ('bronze.crm_prd_info','U')  IS NOT NULL
	DROP TABLE bronze.crm_prd_info;
GO
CREATE TABLE bronze.crm_prd_info(
	prd_id INT,
	prd_key NVARCHAR(50),
	prd_nm NVARCHAR(50),
	prd_cost INT,
	prd_line NVARCHAR(50),
	prd_start_dt DATETIME,
	prd_end_dt DATETIME
);
IF OBJECT_ID ('bronze.crm_cust_info','U')  IS NOT NULL
	DROP TABLE bronze.crm_cust_info;
GO
CREATE TABLE bronze.crm_cust_info(
	cst_id INT,
	cst_key NVARCHAR(50),
	cst_firstname NVARCHAR(50),
	cst_lastname NVARCHAR(50),
	cst_material_status NVARCHAR(50),
	cst_gndr NVARCHAR(50),
	cst_create_date DATE
);
IF OBJECT_ID ('bronze.erp_cust_az12','U')  IS NOT NULL
	DROP TABLE bronze.erp_cust_az12;
GO
CREATE TABLE bronze.erp_cust_az12(
	cid NVARCHAR(50),
	bdate DATE,
	gen NVARCHAR(50)
	);
IF OBJECT_ID ('bronze.erp_loc_a101','U')  IS NOT NULL
	DROP TABLE bronze.erp_loc_a101;

GO 

CREATE TABLE bronze.erp_loc_a101(
	cid NVARCHAR(50),
	cntry NVARCHAR(50)
	);
IF OBJECT_ID ('bronze.erp_px_cat_g1v2','U')  IS NOT NULL
	DROP TABLE bronze.erp_px_cat_g1v2;

GO
CREATE TABLE bronze.erp_px_cat_g1v2(
	id NVARCHAR(50),
	cat NVARCHAR(50),
	subcat NVARCHAR(50),
	maintenance NVARCHAR(50)
	);
