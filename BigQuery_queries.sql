-- number of parks per state
SELECT states as State, count(ParkCode) as num_of_parks 
FROM national_parks.parks 
GROUP BY States
ORDER BY num_of_parks DESC;

-- number of activities per park
SELECT parks.FullName as park, COUNT(activities.activity) as num_activities
FROM national_parks.parks
JOIN national_parks.activities ON parks.ParkCode = activities.ParkCode
GROUP BY park
ORDER BY num_activities DESC;

-- important alerts per park
SELECT parks.fullname as park, alerts.category as alert, alerts.LastIndexedDate as alert_date
FROM national_parks.parks
JOIN national_parks.alerts ON parks.parkcode = alerts.parkcode
WHERE alerts.category LIKE 'Danger' OR alerts.category LIKE 'Caution' 
ORDER BY alert_date DESC;

-- most recent alerts per park
SELECT parks.fullname as park, alerts.category as alert, alerts.LastIndexedDate as alert_date
FROM national_parks.parks
JOIN national_parks.alerts ON parks.parkcode = alerts.parkcode
ORDER BY park, alert_date DESC;

-- parks with their activities
SELECT parks.FullName as park, activities.Activity as activity
FROM national_parks.parks
JOIN national_parks.activities ON parks.ParkCode = activities.ParkCode
WHERE activity IS NOT NULL
ORDER BY park;