CREATE DATABASE etl_project;

CREATE TABLE api_source (
  disclaimer VARCHAR(200) NOT NULL, 
  chart_name VARCHAR(12) NOT NULL, 
  time_updated TIMESTAMP NOT NULL PRIMARY KEY,
  time_updated_iso TIMESTAMP NOT NULL,
  bpi_usd_code VARCHAR(5) NOT NULL,  
  bpi_usd_rate_float FLOAT4 NOT NULL, 
  bpi_usd_description VARCHAR(50) NOT NULL,
  bpi_gbp_code VARCHAR(5) NOT NULL, 
  bpi_gbp_rate_float FLOAT4 NOT NULL, 
  bpi_gbp_description VARCHAR(50) NOT NULL,
  bpi_eur_code VARCHAR(5) NOT NULL, 
  bpi_eur_rate_float FLOAT4 NOT NULL, 
  bpi_eur_description VARCHAR(50) NOT NULL,
  bpi_idr_rate_float FLOAT4 NOT NULL,
  last_update TIMESTAMP NOT NULL
);