import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import configparser
import os
import matplotlib.pyplot as plt
from snowflake.snowpark.session import Session
from snowflake.snowpark import functions as F
from snowflake.snowpark.types import *
import pandas as pd
from snowflake.snowpark.functions import udf

#  Get configs
config = configparser.ConfigParser()
ini_path = os.path.join(os.getcwd(), 'config.ini')
config.read(ini_path)

#  snowflake configs
user = config['SNOWFLAKE']['user']
password = config['SNOWFLAKE']['password']
account = config['SNOWFLAKE']['account']
warehouse = config['SNOWFLAKE']['warehouse']
database = config['SNOWFLAKE']['database']
schema = config['SNOWFLAKE']['schema']

CONNECTION_PARAMS = {
    'user': user,
    'password': password,
    'account': account,
    'warehouse': warehouse,
    'database': database,
    'schema': schema
}

#  Create a session
session = Session.builder.configs(CONNECTION_PARAMS).create()

# Create a DataFrame
df = session.sql("SELECT wkt FROM DM_num_point_zones").collect()
# List to dataframe
df = pd.DataFrame(df, columns=['wkt'])
df['wkt']= gpd.GeoSeries.from_wkt(df['wkt'])
print(df)
my_geo_df = gpd.GeoDataFrame(df, geometry='wkt')
# Plot the geometry data
my_geo_df.plot()

# Customize the plot
plt.title('NYC Taxi Zones')
plt.xlabel('Longitude')
plt.ylabel('Latitude')

# Show the plot
plt.show()
# Close the connection
session.close()




