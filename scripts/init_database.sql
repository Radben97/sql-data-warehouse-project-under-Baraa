/*
=============================================================
Create Database and Schemas
=============================================================
Script Purpose:
    This script creates a new database named 'DataWarehouse' after checking if it already exists. 
    If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas 
    within the database: 'bronze', 'silver', and 'gold'.
	
WARNING:
    Running this script will drop the entire 'DataWarehouse' database if it exists. 
    All data in the database will be permanently deleted. Proceed with caution 
    and ensure you have proper backups before running this script.
*/

IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'dataWarehouse')
BEGIN
	ALTER DATABASE dataWarehouse SET single_user WITH ROLLBACK IMMEDIATE;
	DROP DATABASE dataWarehouse;
END
GO


USE master;

GO

CREATE database dataWarehouse;

GO

USE dataWarehouse;
GO
CREATE schema bronze;
GO
CREATE schema silver;
GO
CREATE schema gold;
