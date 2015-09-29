REGISTER file:/home/hadoop/lib/pig/piggybank.jar
DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader();

leg1flights = LOAD '$INPUT' using CSVLoader AS (Year ,Quarter,Month,DayofMonth,DayOfWeek,FlightDate,UniqueCarrier,AirlineID,Carrier,TailNum,FlightNum,Origin,OriginCityName,OriginState,OriginStateFips,OriginStateName,OriginWac,Dest,DestCityName,DestState,DestStateFips,DestStateName,DestWac,CRSDepTime,DepTime,DepDelay,DepDelayMinutes,DepDel15,DepartureDelayGroups,DepTimeBlk,TaxiOut,WheelsOff,WheelsOn,TaxiIn,CRSArrTime,ArrTime,ArrDelay,ArrDelayMinutes,ArrDel15,ArrivalDelayGroups,ArrTimeBlk,Cancelled,CancellationCode,Diverted,CRSElapsedTime,ActualElapsedTime,AirTime,Flights,Distance,DistanceGroup,CarrierDelay,WeatherDelay,NASDelay,SecurityDelay,LateAircraftDelay);

leg2flights = LOAD '$INPUT' using CSVLoader AS (Year ,Quarter,Month,DayofMonth,DayOfWeek,FlightDate,UniqueCarrier,AirlineID,Carrier,TailNum,FlightNum,Origin,OriginCityName,OriginState,OriginStateFips,OriginStateName,OriginWac,Dest,DestCityName,DestState,DestStateFips,DestStateName,DestWac,CRSDepTime,DepTime,DepDelay,DepDelayMinutes,DepDel15,DepartureDelayGroups,DepTimeBlk,TaxiOut,WheelsOff,WheelsOn,TaxiIn,CRSArrTime,ArrTime,ArrDelay,ArrDelayMinutes,ArrDel15,ArrivalDelayGroups,ArrTimeBlk,Cancelled,CancellationCode,Diverted,CRSElapsedTime,ActualElapsedTime,AirTime,Flights,Distance,DistanceGroup,CarrierDelay,WeatherDelay,NASDelay,SecurityDelay,LateAircraftDelay);


filered_leg1flights = FOREACH leg1flights GENERATE Year,Month,FlightDate,Origin,Dest,DepTime,ArrTime,ArrDelayMinutes,Cancelled,Diverted;

filered_leg2flights = FOREACH leg2flights GENERATE Year,Month,FlightDate,Origin,Dest,DepTime,ArrTime,ArrDelayMinutes,Cancelled,Diverted;

filered_leg1flights = FILTER filered_leg1flights BY (Origin == 'ORD') AND (Dest != 'JFK') AND (Cancelled != 1)  AND (Diverted != 1);

filered_leg2flights = FILTER filered_leg2flights BY (Origin != 'ORD') AND (Dest == 'JFK') AND (Cancelled != 1)  AND (Diverted != 1);


joinedflights = JOIN filered_leg1flights BY (Dest,FlightDate),filered_leg2flights BY (Origin,FlightDate);

flightsjoinedandfilteredbytime = FILTER joinedflights BY (filered_leg1flights::ArrTime < filered_leg2flights::DepTime);


flightsjoinedandfilteredbytimeandyear = FILTER flightsjoinedandfilteredbytime  BY ((filered_leg1flights::Year == 2007 AND

 filered_leg1flights::Month >= 06) OR (filered_leg1flights::Year == 2008 AND filered_leg1flights::Month <= 05)  );


delays = FOREACH flightsjoinedandfilteredbytimeandyear GENERATE filered_leg1flights::ArrDelayMinutes+filered_leg2flights::ArrDelayMinutes;

group_all_delays = GROUP delays ALL;
total_dealys = FOREACH group_all_delays GENERATE SUM(delays)/COUNT(delays);

STORE total_dealys INTO '$OUTPUT/pigVersion1';
