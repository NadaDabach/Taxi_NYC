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
triptype,
current_date() as load_date
FROM {{ source('taxi_source', 'taxi_vert_nyc') }}