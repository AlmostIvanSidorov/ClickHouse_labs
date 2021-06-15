/*Session 2
Lab #1
There are lots of fun things we can now learn about US airline flights.  Build queries that answer the following questions from the default.ontime_pre dataset. 

Which airline had the most flights in 2017?
Which airport has the highest average departure delay?
Can you add the full name to the airport?
Which airport with over 100K flights had the highest departure in 2015?
Which Chicago airport has flights from the most carriers?
Did COVID-19 affect US air travel?  Show your work. :)
(Harder) Are there any airports in default.dot_airports that are missing from default.ontime_pre?

Ошибка в default.ontime_pre*/

DESCRIBE TABLE default.ontime_ref

--Problem 1

SELECT AirlineID,Carrier, COUNT() AS flights
FROM default.ontime_ref
WHERE Year = 2017
GROUP BY AirlineID, Carrier 
ORDER BY flights DESC
limit 1

DESCRIBE TABLE default.dot_airports

--Problem 2

SELECT Name,avg(DepDelayMinutes) AS average_delay
FROM default.ontime_ref o
LEFT JOIN default.dot_airports a ON (o.DestAirportID = a.AirportID)
WHERE Year = 2017
GROUP BY Name
ORDER BY average_delay DESC

--Problem 3

SELECT Origin, count(*) as Flights,avg(DepDelayMinutes) AS average_delay
FROM default.ontime_ref o
LEFT JOIN default.dot_airports a ON (o.DestAirportID = a.AirportID)
WHERE toYear(FlightDate) = 2015
GROUP BY Origin
HAVING Flights > 100000
ORDER BY average_delay DESC

--Problem 4

SELECT City, Name, count(distinct(Carrier))
FROM default.ontime_ref o
LEFT JOIN default.dot_airports a ON (o.DestAirportID = a.AirportID)
WHERE City = 'Chicago'
GROUP BY City, Name
ORDER BY count(Carrier) DESC 

--Problem 5

SELECT Year, count() as Flights,bar(Flights,0,20000000)
FROM default.ontime_ref
where Year > 2010
GROUP BY Year
ORDER by Flights

SELECT toStartOfMonth(FlightDate), count() as Flights,bar(Flights,0,2000000)
FROM default.ontime_ref
where Year > 2010
GROUP BY toStartOfMonth(FlightDate)
ORDER by Flights

--Problem 6

SELECT a.Name
FROM default.ontime_ref o
Right  ANTI JOIN default.dot_airports a ON (a.AirportID = o.OriginAirportID)
GROUP BY  a.Name


/*Lab #2
It can be expensive to calculate cancelled flights from scratch.  Let’s create a materialized view to help. 

Your materialized view should sum up cancelled flights
Roll up by FlightDate and by Carrier
Use a separate table for the data. 
Load the data in from default.ontime_ref
Run queries on both the mat view and ontime_ref.  Which is faster?

Extra credit: How many bytes are in your materialized view?*/

--Problem 1

SELECT FlightDate,Carrier,sum(Cancelled) as Cancelled
FROM default.ontime_ref
GROUP BY FlightDate,Carrier
LIMIT 10000
--973 ms

CREATE TABLE ontime_canceled (
FlightDate  DATE,
Carrier String,
Cancelled AggregateFunction(sum, UInt8)
)
ENGINE = AggregatingMergeTree()
PARTITION BY toYear(FlightDate)
ORDER BY (FlightDate, Carrier)

INSERT INTO ontime_canceled
SELECT FlightDate,Carrier,sumState(Cancelled) as Cancelled
FROM default.ontime_ref
GROUP BY Carrier,FlightDate

CREATE MATERIALIZED VIEW ontime_canceled_mv
TO ontime_canceled
AS SELECT FlightDate,Carrier,sumState(Cancelled) as Cancelled
FROM default.ontime_ref
GROUP BY Carrier,FlightDate
	
SELECT FlightDate,Carrier,sumMerge(Cancelled) as Cancelled
FROM ontime_canceled_mv
GROUP BY FlightDate,Carrier
LIMIT 10000
--525 ms

--Last Problem
SELECT name, total_bytes FROM system.tables
WHERE  database = currentDatabase() and name = 'ontime_canceled'

/*Lab #3
You are almost done!  

Create a local online table by selecting data from 2017 from default.ontime_ref
Put an ngrambf_v1 on the Origin column
Update the column to write the index
Run queries against both tables.  Can you see a difference?  (If not, why not?)
How can you prove that the index is actually there?*/

CREATE TABLE ontime_loc
AS default.ontime_ref
ENGINE = MergeTree
PARTITION BY Year
ORDER BY (Carrier, FlightDate)

INSERT INTO ontime_loc SELECT * from default.ontime_ref WHERE Year = 2017

ALTER TABLE ontime_loc ADD INDEX
  origin_name Origin TYPE ngrambf_v1(3, 512, 2, 0) GRANULARITY 1
  
ALTER TABLE ontime_loc
    MATERIALIZE INDEX origin_name
    
SHOW CREATE TABLE ontime_loc











