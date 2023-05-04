SELECT 
ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS ID,
vendorid,
lpeppickupdatetime,
lpepdropoffdatetime,
passengercount,
tripdistance,
pulocationid,
dolocationid,
ratecodeid,
storeandfwdflag,
paymenttype,
fareamount,
extra,
mtatax,
improvementsurcharge,
tipamount,
tollsamount,
totalamount,
triptype
FROM 
taxi_nyc_database.nadim_schema.taxi_vert_nyc




