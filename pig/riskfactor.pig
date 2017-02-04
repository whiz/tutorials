geolocation = LOAD 'geolocation' USING org.apache.hive.hcatalog.pig.HCatLoader();
normal_events = filter geolocation by event != 'normal';
normal_occurance = foreach normal_events generate driverid, event, (int) '1' as occurance;
normal_occurance_by_driver = group normal_occurance by driverid;
sum_of_normal_events_by_driver = foreach normal_occurance_by_driver generate group as driverid, SUM(normal_occurance.occurance) as t_occ;
drivermileage = LOAD 'drivermileage' using org.apache.hive.hcatalog.pig.HCatLoader();
normal_events_with_mileage = join sum_of_normal_events_by_driver by driverid, drivermileage by driverid;
final_data = foreach normal_events_with_mileage generate $0 as driverid, $1 as events, $3 as totmiles, (float) $3/$1 as riskfactor;
store final_data into 'riskfactor' USING org.apache.hive.hcatalog.HCatStorer(); 
