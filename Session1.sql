--Lab Exercise #1
/*Connect to ClickHouse with your favorite query tool
DBeaver
clickhouse-client
ClickHouse play interface
Find out how many databases there are
Show the tables in your database
Describe the ontime_ref table. It’s in the default database. 
Count the number of rows in the ontime_ref table*/


SHOW DATABASES

SHOW TABLES

DESCRIBE TABLE default.ontime_ref

SELECT COUNT(*) FROM default.ontime_ref

SELECT Carrier, sum(Flights) - sum(Cancelled) AS Flight
FROM default.ontime_ref WHERE Year = 2017
GROUP BY Carrier
ORDER BY Flight DESC LIMIT 1

SELECT sum(Flights) from default.ontime_ref WHERE Year = 2017



/*Lab Exercise #2
Lab #2
Create a table called ontime using the schema of default.ontime_ref
Copy 2017 data from default.ontime_ref to your ontime table
Add a string column called notes to the end of your table
Count the number of rows in your table
How many parts are there in your table?*/

--Problem 1

CREATE TABLE ontime
AS default.ontime_ref
ENGINE = MergeTree
PARTITION BY Year
ORDER BY (Carrier,FlightDate,TailNum)

--Problem 2

INSERT INTO ontime
SELECT * FROM default.ontime_ref 
WHERE Year = 2017

--Problem 3

ALTER TABLE ontime ADD COLUMN notes String

--Problem 4

SELECT COUNT() from ontime 

SELECT COUNT() from default.ontime_ref
WHERE Year = 2017

--Problem 5
SELECT table, count() AS parts 
FROM system.parts
WHERE (database = currentDatabase()) AND active
GROUP BY table ORDER BY table ASC

/*Lab #3
Query system.columns to check compression in the ontime table created in the previous lab exercise
Find a String column that compresses poorly
Find a numeric column that compresses poorly
Make changes to improve compression
Change the String column to use Dictionary Encoding
Change the numeric to use a codec (which one might be best?)
Change the numeric column to use ZSTD compression
Query system.columns again to check compression
Extra credit: Can you think of a query that would help test your improvements?*/

DESC system.columns

select table from system.columns WHERE database = 'vanosidorov_2c665'

--Problem 1

SELECT name,type,compression_codec,data_compressed_bytes,data_uncompressed_bytes,data_uncompressed_bytes/data_compressed_bytes as ratio
FROM system.columns
WHERE database = 'vanosidorov_2c665' AND table = 'ontime'
ORDER BY ratio DESC

--Problem 2

SELECT name,type,data_compressed_bytes,data_uncompressed_bytes,data_uncompressed_bytes/data_compressed_bytes as ratio
FROM system.columns
WHERE database = 'vanosidorov_2c665' AND table = 'ontime' AND type = 'String'
ORDER BY ratio limit 1



SELECT name,type,data_compressed_bytes,data_uncompressed_bytes,data_uncompressed_bytes/data_compressed_bytes as ratio
FROM system.columns
WHERE database = 'vanosidorov_2c665' AND table = 'ontime' AND type = 'String'
ORDER BY data_compressed_bytes DESC limit 2

--Problem 3

SELECT name,type,compression_codec,data_compressed_bytes,data_uncompressed_bytes,data_uncompressed_bytes/data_compressed_bytes as ratio
FROM system.columns
WHERE database = 'vanosidorov_2c665' AND table = 'ontime' AND type = 'Int32'
ORDER BY ratio limit 1

--Problem 4 and others

ALTER TABLE ontime MODIFY COLUMN ActualElapsedTime CODEC(T64,ZSTD(1));

ALTER TABLE ontime UPDATE ActualElapsedTime = ActualElapsedTime WHERE 1

ALTER TABLE ontime ADD COLUMN  DestCityName_lc LowCardinality(String) DEFAULT DestCityName

ALTER TABLE ontime UPDATE DestCityName_lc = DestCityName_lc WHERE 1

SELECT name,type,compression_codec,data_compressed_bytes,data_uncompressed_bytes,data_uncompressed_bytes/data_compressed_bytes as ratio
FROM system.columns
WHERE database = 'vanosidorov_2c665' AND table = 'ontime' AND name LIKE'DestCityName%'
ORDER BY data_compressed_bytes DESC

ALTER TABLE ontime ADD COLUMN  OriginCityName_codec String DEFAULT OriginCityName

ALTER TABLE ontime MODIFY COLUMN OriginCityName_codec CODEC(ZSTD(1));

ALTER TABLE ontime UPDATE OriginCityName_codec = OriginCityName_lc WHERE 1

SELECT name,compression_codec,type,data_compressed_bytes,data_uncompressed_bytes,data_uncompressed_bytes/data_compressed_bytes as ratio
FROM system.columns
WHERE database = 'vanosidorov_2c665' AND table = 'ontime' AND name LIKE'OriginCityName%'
ORDER BY data_compressed_bytes DESC

