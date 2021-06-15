/*Session 3
Lab #1
Let’s do a variation of the example to track aircraft destinations. 
How many destinations did aircraft N223WN visit on January 15, 2017? Use FlightDate and TailNum to identify the relevant flights. 
Add a GROUP BY and groupArray() to turn the destinations into an array. 
Can you create a query that returns the aircraft tail number, the first destination *and* the last destination in a single output row?*/

SELECT FlightDate, TailNum, groupArray(Name) as Dests
FROM default.ontime_ref o
LEFT JOIN default.dot_airports a ON (o.DestAirportID = a.AirportID)
WHERE (FlightDate = toDate('2017-01-15')) AND (TailNum = 'N223WN')
GROUP BY FlightDate, TailNum

SELECT TailNum, toString(groupArray(Dest))
FROM default.ontime_ref
WHERE (FlightDate = toDate('2017-01-15')) AND (TailNum = 'N223WN')
GROUP BY TailNum


WITH groupArray(Name) as array
SELECT FlightDate, TailNum,array[1] as firstPlace,array[-1] as lastPlace
FROM default.ontime_ref o
LEFT JOIN default.dot_airports a ON (o.DestAirportID = a.AirportID)
WHERE (FlightDate = toDate('2017-01-15')) AND (TailNum = 'N223WN')
GROUP BY FlightDate, TailNum


SELECT FlightDate, TailNum,Dest, Name
FROM default.ontime_ref o
LEFT JOIN default.dot_airports a ON (o.DestAirportID = a.AirportID)
WHERE (FlightDate = toDate('2017-01-15')) AND (TailNum = 'N223WN')
GROUP BY FlightDate, TailNum,Dest, Name



SELECT TailNum,Destan[1] as firstPlace, Destan[-1] as lastPlace
FROM 
(
SELECT TailNum,
arraySort((x,y) -> y,groupArray(Dest), groupArray(ArrTime)) as Destan
FROM default.ontime_ref
WHERE (FlightDate = toDate('2017-01-15')) AND (TailNum = 'N223WN')
GROUP BY TailNum
)

/*Lab #2
Let’s load some JSON data and turn it into nice columns. 
Create a new table from default.http_logs in your database.
Copy 1000000 rows from default.http_logs into your own table. (Use LIMIT!)
Select a few rows of data and see what the JSON looks like. 
Select the count of different status codes directly from JSON.
Add a column to materialize the status codes in the table. 
Write the same query using your new column.  Is the answer the same as #4?*/

SHOW CREATE TABLE default.http_logs

CREATE TABLE http_logs as
default.http_logs
ENGINE = MergeTree
PARTITION BY file
ORDER BY tuple()

INSERT INTO http_logs SELECT * FROM default.http_logs LIMIT 1000000

SELECT * FROM http_logs LIMIT 2

SELECT sum(JSONExtractInt(message, 'status')) AS Client_IP
FROM http_logs LIMIT 3

SELECT JSONExtractRaw(message, 'clientip') AS Client_IP from http_logs LIMIT 3

ALTER TABLE http_logs ADD COLUMN status Int32 DEFAULT JSONExtractInt(message, 'status')

ALTER TABLE http_logs DROP COLUMN status

ALTER TABLE http_logs ADD COLUMN clientip String DEFAULT JSONExtractRaw(message, 'clientip')

ALTER TABLE http_logs UPDATE clientip = clientip WHERE 1

ALTER TABLE http_logs ADD COLUMN clientipM String MATERIALIZED JSONExtractRaw(message, 'clientip')

OPTIMIZE TABLE http_logs FINAL

SELECT clientipM FROM http_logs LIMIT 2

SELECT sum(status) AS Status
FROM http_logs LIMIT 3

SELECT clientip AS Client_IP
FROM http_logs LIMIT 3























