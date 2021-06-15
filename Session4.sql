/*Lab #1
Let’s create replicated tables! 

Find out which cluster layouts are available using system tables.
Create a replicated table using the default.ontime_ref schema on cluster clickhouse101.
Create a distributed table to find data in the replicated table.
Insert data from 2017 using the distributed table. 
Count the rows via the distributed table and the local table. 
Are they the same?*/

SELECT cluster, groupArray(host_name) AS hosts
FROM system.clusters 
GROUP BY cluster ORDER BY cluster

SHOW CREATE TABLE default.ontime_ref

CREATE TABLE IF NOT EXISTS ontime_shard2 ON CLUSTER `{cluster}` 
AS vanosidorov_2c665.ontime
Engine=ReplicatedMergeTree(
'/clickhouse/{cluster}/tables/{shard}/vanosidorov_2c665/ontime_shard2', '{replica}')
PARTITION BY toYYYYMM(FlightDate)
ORDER BY (FlightDate, `Year`, `Month`, DepDel15)


CREATE TABLE IF NOT EXISTS ontime5 ON CLUSTER '{cluster}'
AS vanosidorov_2c665.ontime_shard2
ENGINE = Distributed(
  '{cluster}', currentDatabase(), ontime_shard2, rand())
  
INSERT INTO ontime_shard2 SELECT * from vanosidorov_2c665.ontime
WHERE Year = 2017 LIMIT 1000000

SELECT * from ontime5 limit 10

SELECT * from ontime_shard2 limit 10





