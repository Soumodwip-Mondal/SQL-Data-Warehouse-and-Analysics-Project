/*
=============================================================
Create Database and Schemas
=============================================================
Script purpose:
Creating the database exists or not. If the database exists then the command will drop and recreate it.
Also, it will create the bronze, silver, gold layer schemas.
*/

DROP DATABASE IF EXISTS data_warehouse; 

CREATE DATABASE data_warehouse;

--Create the bronze, silver, gold layer schemas one by one 
CREATE SCHEMA bronze_layer;

CREATE SCHEMA silver_layer;

CREATE SCHEMA gold_layer;