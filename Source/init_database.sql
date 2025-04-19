/*
========================================================
Create database and schema
========================================================
this scrip is use to create database datawarehouse and have three schema - bronze, silver and gold
*/

USE master;
GO
-- CREATE DATAWAREHOUSE
Create database datawarehouse;

--GO INTO THE DATAWAREHOUSE
use datawarehouse;

--CREATE SCHEMA WITH 3 DIFFERENT NAME(BRONZE, SILVER & GOLD)
Create schema bronze; 
Go
Create schema Silver; 
GO
Create schema Gold; 
GO
