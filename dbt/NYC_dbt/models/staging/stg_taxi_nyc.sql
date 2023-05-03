SELECT 
ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS ID, *
FROM 
taxi_nyc_database.nadim_schema.taxi_vert_nyc