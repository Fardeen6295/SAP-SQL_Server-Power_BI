/*ETL Control for Incremental Load Whenever Possible */

IF NOT EXISTS (
	SELECT 1
	FROM sys.tables 
	WHERE name = 'ETL_Control'
	AND schema_id = SCHEMA_ID('dbo')
)
BEGIN
	CREATE TABLE [dbo].[ETL_Control](
		[TableName] [varchar](30) NOT NULL,
		[LastLoadDate] [varchar](20) NULL
	);
END

/* Customers */

IF NOT EXISTS (
	SELECT 1
	FROM sys.tables 
	WHERE name = 'customer'
	AND schema_id = SCHEMA_ID('brz')
)
BEGIN
	CREATE TABLE [brz].[customer](
		[client_id] [int] NOT NULL,
		[customer_id] [varchar](20) NULL,
		[customer_name] [varchar](100) NULL,
		[city] [varchar](100) NULL,
		[country] [varchar](3) NULL,
		[created_on] [varchar](10) NULL
	);
END



IF NOT EXISTS (
	SELECT 1
	FROM sys.tables 
	WHERE name = 'customer'
	AND schema_id = SCHEMA_ID('stg')
)
BEGIN
	CREATE TABLE [stg].[customer](
		[client_id] [int] NOT NULL,
		[customer_id] [varchar](20) NULL,
		[customer_name] [varchar](100) NULL,
		[city] [varchar](100) NULL,
		[country] [varchar](30) NULL,
		[created_on] [DATE] NULL,
		modified_on [DATETIME]
	);
END



IF NOT EXISTS (
	SELECT 1
	FROM sys.tables 
	WHERE name = 'customer'
	AND schema_id = SCHEMA_ID('sil')
)
BEGIN
	CREATE TABLE [sil].[customer](
		[client_id] [int] NOT NULL,
		[customer_id] [varchar](20) NULL,
		[customer_name] [varchar](100) NULL,
		[city] [varchar](100) NULL,
		[country] [varchar](30) NULL,
		[created_on] [DATE] NULL,
		modified_on [DATETIME]
	);
END


IF NOT EXISTS (
	SELECT 1
	FROM sys.tables 
	WHERE name = 'DimCustomer'
	AND schema_id = SCHEMA_ID('gold')
)
BEGIN
	CREATE TABLE [gold].[DimCustomer](
		[DimCustomerKey] INT IDENTITY(1,1) PRIMARY KEY,
		[CustomerID] [varchar](20) NULL,
		[CustomerName] [varchar](100) NULL,
		[City] [varchar](100) NULL,
		[Country] [varchar](30) NULL,
		[created_on] [DATE] NULL,
		modified_on [DATETIME]
	);
END

/*
MERGE IN SILVER 

MERGE sil.customer as trg
USING stg.customer as src
ON (trg.customer_id = src.customer_id)


WHEN MATCHED
AND (
	trg.customer_name <> src.customer_name OR
	trg.city <> src.city OR
	trg.country <> src.country
)
THEN 
UPDATE SET
	trg.customer_name = src.customer_name,
		trg.city = src.city,
		trg.country = src.country,
		trg.modified_on = src.modified_on,
		trg.created_on = src.created_on

WHEN NOT MATCHED BY TARGET THEN
INSERT(client_id, customer_id, customer_name, city, country, created_on, modified_on)
VALUES(src.client_id, src.customer_id, src.customer_name, src.city, src.country, src.created_on, src.modified_on);
*/


/* MERGE IN GOLD CUSTOMER

MERGE gold.DimCustomer as trg
USING sil.customer as src
ON trg.CustomerID = src.customer_id

WHEN MATCHED 
AND(
	trg.CustomerName <> src.customer_name OR
	trg.City <> src.city OR
	trg.Country <> src.country
)
THEN UPDATE SET
	trg.CustomerName = src.customer_name,
	trg.City = src.city,
	trg.Country = src.country,
	trg.created_on = src.created_on,
	trg.modified_on = src.modified_on

WHEN NOT MATCHED BY TARGET THEN
	INSERT (CustomerID, CustomerName, City, Country, created_on, modified_on)
	VALUES (src.customer_id, src.customer_name, src.city, src.country, src.created_on, src.modified_on);

*/


/* Materials */

IF NOT EXISTS (
	SELECT 1
	FROM sys.tables 
	WHERE name = 'material'
	AND schema_id = SCHEMA_ID('brz')
)
BEGIN
	CREATE TABLE [brz].[material](
		[client_id] [int] NOT NULL,
		[material_id] [varchar](20) NULL,
		[material_type] [varchar](100) NULL,
		[material_description] [varchar](100) NULL,
		[base_unit] [varchar](30) NULL,
		[created_on] DATETIME DEFAULT CURRENT_TIMESTAMP
	);
END



IF NOT EXISTS (
	SELECT 1
	FROM sys.tables 
	WHERE name = 'material'
	AND schema_id = SCHEMA_ID('sil')
)
BEGIN
	CREATE TABLE [sil].[material](
		[client_id] [int] NOT NULL,
		[material_id] [varchar](20) NULL,
		[material_type] [varchar](100) NULL,
		[material_description] [varchar](100) NULL,
		[base_unit] [varchar](30) NULL,
		[created_on] DATETIME,
		[StartDate] [DATETIME] DEFAULT CURRENT_TIMESTAMP NULL,
		[EndDate] [DATETIME] DEFAULT '3000-12-31 00:00:00.000' NULL,
		[IsCurrent] CHAR(1) DEFAULT 'Y'
	);
END


IF NOT EXISTS (
	SELECT 1
	FROM sys.tables 
	WHERE name = 'DimMaterial'
	AND schema_id = SCHEMA_ID('gold')
)
BEGIN
	CREATE TABLE [gold].[DimMaterial](
		[DimMaterialKey] INT IDENTITY(1,1) PRIMARY KEY,
		[MaterialID] [varchar](20) NULL,
		[MaterialType] [VARCHAR](50) NULL,
		[MaterialDescription] [varchar](100) NULL,
		[BaseUnit] [varchar](100) NULL,
		[CreatedOn] [DATETIME] NULL,
		[StartDate] [DATETIME] NULL,
		[EndDate] [DATETIME] NULL,
		[IsCurrent] CHAR(1)
	);
END

/*
MERGE  IN gold.DimMaterial USING sil.material
BEGIN TRANSACTION; 

MERGE gold.DimMaterial as trg
USING sil.material as src
ON trg.MaterialID = src.material_id AND trg.IsCurrent = 'Y'
WHEN MATCHED AND (
	trg.MaterialType <> src.material_type OR
	trg.MaterialDescription <> src.material_description OR
	trg.BaseUnit <> src.base_unit
)
THEN UPDATE SET
	trg.EndDate = CURRENT_TIMESTAMP,
	trg.IsCurrent = 'N';

MERGE gold.DimMaterial as trg 
USING sil.material as src
ON trg.MaterialID = src.material_id
AND trg.IsCurrent = 'Y'
WHEN NOT MATCHED THEN 
	INSERT (MaterialID, MaterialType, MaterialDescription, BaseUnit, CreatedOn, StartDate, EndDate, IsCurrent)
	VALUES(src.material_id, src.material_type, src.material_description, src.base_unit, src.created_on, src.StartDate, src.EndDate, src.IsCurrent);

COMMIT TRANSACTION;
*/

/* Order */

IF NOT EXISTS (
	SELECT 1
	FROM sys.tables 
	WHERE name = 'order'
	AND schema_id = SCHEMA_ID('brz')
)
BEGIN
	CREATE TABLE [brz].[order](
		[client_id] [int] NOT NULL,
		[order_id] [varchar](20) NULL,
		[customer_id] [varchar](100) NULL,
		[created_on] [varchar](10) NULL,
		[modified_on] [varchar](10) NULL,
		[amount] [DECIMAL](10,2) NULL,
		[currency] [varchar](5) NULL
	);
END



IF NOT EXISTS (
	SELECT 1
	FROM sys.tables 
	WHERE name = 'order'
	AND schema_id = SCHEMA_ID('stg')
)
BEGIN
	CREATE TABLE [stg].[order](
		[client_id] [int] NOT NULL,
		[order_id] [varchar](20) NULL,
		[customer_id] [varchar](100) NULL,
		[created_on] [DATE] NULL,
		[modified_on] [DATE] NULL,
		[amount] [DECIMAL](10,2) NULL,
		[currency] [varchar](5) NULL
	);
END



IF NOT EXISTS (
	SELECT 1
	FROM sys.tables 
	WHERE name = 'order'
	AND schema_id = SCHEMA_ID('sil')
)
BEGIN
	CREATE TABLE [sil].[order](
		[client_id] [int] NOT NULL,
		[order_id] [varchar](20) NULL,
		[customer_id] [varchar](100) NULL,
		[created_on] [DATE] NULL,
		[modified_on] [DATE] NULL,
		[amount] [DECIMAL](10,2) NULL,
		[currency] [varchar](5) NULL
	);
END


/*
BEGIN TRANSACTION;

MERGE sil.[order] as trg
USING stg.[order] as src
ON trg.order_id = src.order_id
WHEN MATCHED AND (
	trg.customer_id <> src.customer_id OR
	trg.modified_on <> src.modified_on OR
	trg.amount <> src.amount
)
THEN UPDATE SET
	trg.customer_id = src.customer_id,
	trg.modified_on = src.modified_on,
	trg.amount = src.amount
WHEN NOT MATCHED BY TARGET THEN
INSERT (client_id, order_id, customer_id, created_on, modified_on, amount, currency)
VALUES (src.client_id, src.order_id, src.customer_id, src.created_on, src.modified_on, src.amount, src.currency);

COMMIT TRANSACTION;
*/

IF NOT EXISTS (
	SELECT 1
	FROM sys.views 
	WHERE name = 'vw_factorder_input'
	AND schema_id = SCHEMA_ID('sil')
)

BEGIN
EXEC('
	CREATE VIEW [sil].[vw_factorder_input]
AS
SELECT 
	o.order_id, 
	c.DimCustomerKey,
	o.created_on,
	o.modified_on,
	o.amount,
	o.currency
FROM
	stg.[order] as o
LEFT JOIN 
	gold.DimCustomer as c
	ON c.CustomerID = o.customer_id
');
END



IF NOT EXISTS (
	SELECT 1
	FROM sys.tables 
	WHERE name = 'FactOrder'
	AND schema_id = SCHEMA_ID('gold')
)
BEGIN
	CREATE TABLE [gold].[FactOrder](
		[FactOrderKey] INT IDENTITY(1,1) PRIMARY KEY,
		[OrderID] [varchar](20) NULL,
		[DimCustomerKey] [varchar](100) NULL,
		[CreatedOn] [DATE] NULL,
		[ModifiedOn] [DATE] NULL,
		[Amount] [DECIMAL](10,2) NULL,
		[Currency] [varchar](5) NULL
	);
END

/*

BEGIN TRANSACTION;

MERGE gold.FactOrder AS trg
USING sil.vw_factorder_input as src
ON trg.OrderID = src.order_id
WHEN MATCHED AND(
	ISNULL(trg.DimCustomerKey, -1) <> ISNULL(src.DimCustomerKey, -1) OR
	ISNULL(trg.ModifiedOn, '1900-01-01') <> ISNULL(src.modified_on, '1900-01-01') OR
	ISNULL(trg.Amount, 0) <> ISNULL(src.amount, 0) OR
	ISNULL(trg.Currency, 'ABC') <> ISNULL(src.currency, 'ABC')
)
THEN UPDATE SET
	trg.DimCustomerKey = src.DimCustomerKey,
	trg.ModifiedOn = src.modified_on,
	trg.Amount = src.amount,
	trg.Currency = src.currency
WHEN NOT MATCHED BY TARGET THEN
INSERT (OrderID, DimCustomerKey, CreatedOn, ModifiedOn, Amount, Currency)
VALUES (src.order_id, src.DimCustomerKey, src.created_on, src.modified_on, src.amount, src.currency);

COMMIT TRANSACTION;

*/

/* Order Details */

IF NOT EXISTS (
	SELECT 1
	FROM sys.tables 
	WHERE name = 'orderDetail'
	AND schema_id = SCHEMA_ID('brz')
)
BEGIN
	CREATE TABLE [brz].[orderDetail](
		[client_id] [int] NOT NULL,
		[order_id] [varchar](20) NULL,
		[item_id] [varchar](100) NULL,
		[material_id] [varchar](20) NULL,
		[quantity] [Decimal](10,2) NULL,
		[amount] [DECIMAL](10,2) NULL
	);
END



IF NOT EXISTS (
	SELECT 1
	FROM sys.tables 
	WHERE name = 'orderDetail'
	AND schema_id = SCHEMA_ID('stg')
)
BEGIN
	CREATE TABLE [stg].[orderDetail](
		[client_id] [int] NOT NULL,
		[order_id] [varchar](20) NULL,
		[item_id] [varchar](100) NULL,
		[material_id] [varchar](20) NULL,
		[quantity] [Decimal](10,2) NULL,
		[amount] [DECIMAL](10,2) NULL
	);
END



IF NOT EXISTS (
	SELECT 1
	FROM sys.tables 
	WHERE name = 'orderDetail'
	AND schema_id = SCHEMA_ID('sil')
)
BEGIN
	CREATE TABLE [sil].orderDetail(
		[client_id] [int] NOT NULL,
		[order_id] [varchar](20) NULL,
		[item_id] [varchar](100) NULL,
		[material_id] [varchar](20) NULL,
		[quantity] [Decimal](10,2) NULL,
		[amount] [DECIMAL](10,2) NULL
	);
END

/*
BEGIN TRANSACTION;

MERGE sil.orderDetail as trg
USING stg.orderDetail as src
ON trg.order_id = src.order_id AND trg.item_id = src.item_id

WHEN MATCHED AND(
	trg.material_id <> src.material_id OR
	trg.quantity <> src.quantity OR
	trg.amount <> src.amount
) THEN UPDATE SET
	trg.material_id = src.material_id,
	trg.quantity = src.quantity,
	trg.amount = src.amount

WHEN NOT MATCHED BY TARGET THEN
INSERT (client_id, order_id, item_id, material_id, quantity, amount)
VALUES (src.client_id, src.order_id, src.item_id, src.material_id, src.quantity, src.amount);

COMMIT TRANSACTION;
*/

IF NOT EXISTS (
	SELECT 1
	FROM sys.views 
	WHERE name = 'vw_factorderdetail_input'
	AND schema_id = SCHEMA_ID('sil')
)

BEGIN
EXEC('CREATE VIEW sil.vw_factorderdetail_input
AS
WITH CTE AS (
	SELECT 
		od.order_id, od.item_id, od.material_id, od.quantity, od.amount, 
		o.created_on, o.modified_on
	FROM 
		stg.orderDetail as od
	INNER JOIN 
		stg.[order] as o
		ON od.order_id = o.order_id
)
SELECT 
	CTE.order_id, CTE.item_id, ISNULL(dm.DimMaterialKey, -1) AS DimMaterialKey,
	CTE.quantity, CTE.amount, CTE.created_on, CTE.modified_on
FROM CTE
LEFT JOIN 
	gold.DimMaterial as dm
	ON CTE.material_id = dm.MaterialID
	AND CTE.created_on >= dm.StartDate
	AND CTE.created_on < dm.EndDate
')
END;

IF NOT EXISTS (
	SELECT 1
	FROM sys.tables 
	WHERE name = 'FactOrderDetail'
	AND schema_id = SCHEMA_ID('gold')
)
BEGIN
	CREATE TABLE [gold].[FactOrderDetail](
		[FactOrderDetailKey] INT IDENTITY(1,1) PRIMARY KEY,
		[OrderID] [varchar](20) NULL,
		[ItemID] [VARCHAR](30) NULL,
		[DimMaterialKey] [INT] NULL,
		[Quantity] [DECIMAL](10,4) NULL,
		[Amount] [DECIMAL](10,2) NULL,
		[CreatedOn] [DATE] NULL,
		[ModifiedOn] [DATE] NULL
	);
END


/*
BEGIN TRANSACTION;

MERGE gold.FactOrderDetail AS trg
USING sil.vw_factorderdetail_input AS src
ON trg.OrderID = src.order_id AND trg.ItemID = src.item_id
WHEN MATCHED AND(
	trg.DimMaterialKey <> src.DimMaterialKey OR
	trg.Quantity <> src.quantity OR
	trg.Amount <> src.amount OR
	trg.ModifiedOn <> src.modified_on
)
THEN UPDATE SET
	trg.DimMaterialKey = src.DimMaterialKey,
	trg.Quantity = src.quantity,
	trg.Amount = src.amount,
	trg.ModifiedOn = src.modified_on

WHEN NOT MATCHED BY TARGET THEN
INSERT (OrderID, ItemID ,DimMaterialKey, Quantity, Amount, CreatedOn, ModifiedOn)
VALUES (src.order_id, src.item_id, src.DimMaterialKey, src.quantity, src.amount, src.created_on, src.modified_on);

COMMIT TRANSACTION;

*/