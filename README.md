# SAP-SQL_Server-Power_BI

## Technologies Used for the Project
* SAP Sybase as Source of Raw Data Hosted on AWS VM
* Virtual Machine with these Softwares for Development provided by Client on Client's Local Network
  * SQL Server 2019
  * Visual Studio 2019
  * SQL Server Integration Services Projects
  * SQL Server Management Stuido 20
  * On Prem Data Gateway
  * Power BI Desktop
  * Power BI Service Account

<img width="1575" height="467" alt="Untitled-2025-12-23-1756" src="https://github.com/user-attachments/assets/4e0f3d95-180b-4f77-9848-72684c1e8ea6" />

### Quick Explanation of the Project Requirement 
* Client wanted a Cost effective approach to create power bi reports and dashboard by avoiding cloud storage and resource
* For that We had to use On Prem SQL Server and SSIS for creating Pipeline for Data Warehouse On Prem
* Then Used that On Prem SQL Server as Data Source for Power BI Reports and Dashboards
* SQL Server Have One Database that have all schemas in it Bronze(brz), Stage(stg), Silvse(sil), Gold(gold), DBO(dbo)
* This is not the Complete Project I had build for the Client just a sample of the project
* It Showcase my skills of different Languages and tools such as
  *  SQL
  *  T-SQL
  *  SSIS
  *  SSMS
  *  Incremental Load of Data for Data Warehouse
  *  Power BI
  *  DAX
  *  UI/UX
  *  End-to-ENd Solution from SAP to Data Warehouse On Prem to Power BI Reports and Dashboards
 
## Developemt of SSIS Packages (Bronze Layer)
Bronze Layer is Used to Load data from
SAP Sybase to brz schema tables incremnetally isung watermark approach of dbo.etl_control table

<img width="903" height="659" alt="{E1DD0294-9EB4-4266-AEDF-6F836121161B}" src="https://github.com/user-attachments/assets/4af14e1a-5a91-4b9c-a621-52d6c56a163d" />


Below are the variables used for the tables which are required to load data incremnetally most fact tables


<img width="1318" height="238" alt="{CC73831C-F6F0-4BF9-9744-2541D64F7FC9}" src="https://github.com/user-attachments/assets/bcfcb11b-3775-4e55-a59b-fd2cb3d16f14" />



## Developemt of SSIS Packages (Silver Layer)
Silver Layer is used for Transforming brz incremental data and clean it and then load it in stg schema tables
stg schema tables are then used to Merge/Upsert in sil schema tables
brz and stg tables are transient layer but silver and gold tables hold complete data and stg is used to Upsert Silver tables


<img width="796" height="655" alt="{387B9C95-BC95-46FD-925D-F2DB47085FF2}" src="https://github.com/user-attachments/assets/f3f1ef14-31f7-4e6c-b59b-70c4173449ae" />


Sample of Data Flow 


<img width="311" height="469" alt="{4048A067-07BE-4373-936E-5731F6D49C63}" src="https://github.com/user-attachments/assets/8ff1284d-8ee3-44e9-8eed-04c78533799c" />

Here is the Sample of Order table Merge statement used to Upsert sil.order from stg.order that have new and modified records 

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


## Developemt of SSIS Packages (Gold Layer)


<img width="1135" height="544" alt="{C441BE6F-A447-48CE-A3FD-7140E0266F46}" src="https://github.com/user-attachments/assets/82b11df5-6ae5-473c-baa7-65d12e5efbd3" />



Here we are creating Data Model with Surrogate Keys in DIm tables and linking them to fact with joins and selecting Surrogate keys in facts not business keys for better joins performance

In Gold Layer we had 2 differnt approach for tables
* Fact Tables
* Dim Tables

Let's cover Dim Tables, we created 2 types of Dim tables
* DimCustomer of SCD Type 1
* DimMaterial of SCD Type 2


### gold.DimCustomer

We created gold.DimCustomer SCD Type 1 from sil.customer and Merged using sil.customer table, you can find the code below 


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


### gold.DimMaterial

We created gold.DimMaterial SCD Type 2 from sil.Material and Merged using sil.customer table, you can find the code below 
Note: sil.material had all the extra column dwfinations like StartDate, EndDate, IsCurrent etc with Default value as Current_Timestamp, '3000-12-31', 'Y' Respectively


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

