-- RusTs Benchmark Queries
-- Adapted from ClickBench for NYC TLC HVFHS trip data
-- Source: https://github.com/ClickHouse/ClickBench
--
-- Trip data schema (HVFHS - High Volume For-Hire Services):
--   hvfhs_license_num, dispatching_base_num, originating_base_num,
--   request_datetime, on_scene_datetime, pickup_datetime, dropoff_datetime,
--   PULocationID, DOLocationID, trip_miles, trip_time,
--   base_passenger_fare, tolls, bcf, sales_tax, congestion_surcharge,
--   airport_fee, tips, driver_pay, shared_request_flag, shared_match_flag,
--   access_a_ride_flag, wav_request_flag, wav_match_flag

-- Q0: Simple count - baseline scan
SELECT COUNT(*) FROM trips;

-- Q1: Count with simple filter
SELECT COUNT(*) FROM trips WHERE trip_miles > 0;

-- Q2: Sum of numeric field
SELECT SUM(trip_miles) FROM trips;

-- Q3: Average calculation
SELECT AVG(base_passenger_fare) FROM trips;

-- Q4: Count distinct on categorical field
SELECT COUNT(DISTINCT hvfhs_license_num) FROM trips;

-- Q5: Count distinct on location
SELECT COUNT(DISTINCT PULocationID) FROM trips;

-- Q6: Min/Max aggregation
SELECT MIN(trip_miles), MAX(trip_miles) FROM trips;

-- Q7: Filter with range condition
SELECT COUNT(*) FROM trips WHERE trip_miles >= 1 AND trip_miles <= 10;

-- Q8: Filter on string equality
SELECT COUNT(*) FROM trips WHERE hvfhs_license_num = 'HV0003';

-- Q9: Multiple conditions with AND
SELECT COUNT(*) FROM trips WHERE trip_miles > 5 AND base_passenger_fare > 20;

-- Q10: Group by single field with count
SELECT hvfhs_license_num, COUNT(*) FROM trips GROUP BY hvfhs_license_num;

-- Q11: Group by with sum
SELECT hvfhs_license_num, SUM(trip_miles) FROM trips GROUP BY hvfhs_license_num;

-- Q12: Group by with average
SELECT PULocationID, AVG(base_passenger_fare) FROM trips GROUP BY PULocationID;

-- Q13: Group by with ORDER BY count DESC, LIMIT
SELECT PULocationID, COUNT(*) FROM trips GROUP BY PULocationID ORDER BY COUNT(*) DESC LIMIT 10;

-- Q14: Group by with ORDER BY sum DESC, LIMIT
SELECT PULocationID, SUM(trip_miles) FROM trips GROUP BY PULocationID ORDER BY SUM(trip_miles) DESC LIMIT 10;

-- Q15: Multiple aggregations in single query
SELECT COUNT(*), SUM(trip_miles), AVG(base_passenger_fare), MIN(tips), MAX(driver_pay) FROM trips;

-- Q16: Group by with multiple aggregations
SELECT hvfhs_license_num, COUNT(*), SUM(trip_miles), AVG(base_passenger_fare) FROM trips GROUP BY hvfhs_license_num;

-- Q17: Filter + group by + order by + limit (top pickup locations by revenue)
SELECT PULocationID, SUM(base_passenger_fare) FROM trips WHERE trip_miles > 0 GROUP BY PULocationID ORDER BY SUM(base_passenger_fare) DESC LIMIT 10;

-- Q18: Filter + group by + order by + limit (top dropoff locations by count)
SELECT DOLocationID, COUNT(*) FROM trips WHERE trip_time > 600 GROUP BY DOLocationID ORDER BY COUNT(*) DESC LIMIT 10;

-- Q19: Two-column group by
SELECT hvfhs_license_num, PULocationID, COUNT(*) FROM trips GROUP BY hvfhs_license_num, PULocationID;

-- Q20: Two-column group by with filter and limit
SELECT hvfhs_license_num, PULocationID, COUNT(*) FROM trips WHERE trip_miles > 5 GROUP BY hvfhs_license_num, PULocationID ORDER BY COUNT(*) DESC LIMIT 20;

-- Q21: Sum with filter on multiple conditions
SELECT SUM(base_passenger_fare) FROM trips WHERE hvfhs_license_num = 'HV0003' AND trip_miles > 2;

-- Q22: Average fare and miles separately (arithmetic in SELECT not yet supported)
SELECT AVG(base_passenger_fare), AVG(trip_miles) FROM trips WHERE trip_miles > 0;

-- Q23: Count with boolean-like flag filter
SELECT COUNT(*) FROM trips WHERE shared_request_flag = 'Y';

-- Q24: Count with WAV (wheelchair accessible) filter
SELECT COUNT(*) FROM trips WHERE wav_match_flag = 'Y';

-- Q25: Group by dispatching base
SELECT dispatching_base_num, COUNT(*) FROM trips GROUP BY dispatching_base_num ORDER BY COUNT(*) DESC LIMIT 10;

-- Q26: Top routes (pickup to dropoff)
SELECT PULocationID, DOLocationID, COUNT(*) FROM trips GROUP BY PULocationID, DOLocationID ORDER BY COUNT(*) DESC LIMIT 20;

-- Q27: Revenue by license with filter
SELECT hvfhs_license_num, SUM(base_passenger_fare), SUM(tips), SUM(driver_pay) FROM trips WHERE trip_miles > 1 GROUP BY hvfhs_license_num;

-- Q28: Long trips analysis
SELECT COUNT(*), AVG(trip_miles), AVG(trip_time), AVG(base_passenger_fare) FROM trips WHERE trip_miles > 20;

-- Q29: Short trips analysis
SELECT COUNT(*), AVG(trip_miles), AVG(trip_time), AVG(base_passenger_fare) FROM trips WHERE trip_miles < 2;

-- Q30: Airport fee analysis (trips with airport fees)
SELECT COUNT(*), SUM(airport_fee), AVG(airport_fee) FROM trips WHERE airport_fee > 0;

-- Q31: Congestion surcharge analysis
SELECT COUNT(*), SUM(congestion_surcharge), AVG(congestion_surcharge) FROM trips WHERE congestion_surcharge > 0;

-- Q32: Tolls analysis
SELECT COUNT(*), SUM(tolls), AVG(tolls) FROM trips WHERE tolls > 0;

-- Q33: Tips analysis by license
SELECT hvfhs_license_num, SUM(tips), AVG(tips), COUNT(*) FROM trips WHERE tips > 0 GROUP BY hvfhs_license_num;

-- Q34: Driver pay analysis
SELECT MIN(driver_pay), MAX(driver_pay), AVG(driver_pay), SUM(driver_pay) FROM trips;

-- Q35: BCF (Black Car Fund) analysis
SELECT SUM(bcf), AVG(bcf) FROM trips WHERE bcf > 0;

-- Q36: Sales tax analysis
SELECT SUM(sales_tax), AVG(sales_tax) FROM trips WHERE sales_tax > 0;

-- Q37: Top earning routes
SELECT PULocationID, DOLocationID, SUM(base_passenger_fare), AVG(trip_miles) FROM trips GROUP BY PULocationID, DOLocationID ORDER BY SUM(base_passenger_fare) DESC LIMIT 20;

-- Q38: Efficiency analysis (fare and time separately)
SELECT hvfhs_license_num, AVG(base_passenger_fare), AVG(trip_time) FROM trips WHERE trip_time > 0 GROUP BY hvfhs_license_num;

-- Q39: Access-a-ride trips
SELECT COUNT(*), AVG(trip_miles), AVG(base_passenger_fare) FROM trips WHERE access_a_ride_flag = 'Y';

-- Q40: Shared rides analysis
SELECT shared_match_flag, COUNT(*), AVG(trip_miles), AVG(base_passenger_fare) FROM trips GROUP BY shared_match_flag;

-- Q41: Simple LIMIT (test LIMIT push-down)
SELECT * FROM trips LIMIT 10;

-- Q42: LIMIT with ORDER BY DESC (test descending heap)
SELECT * FROM trips ORDER BY time DESC LIMIT 10;

-- Q43: Large LIMIT
SELECT * FROM trips LIMIT 1000;
