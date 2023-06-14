import configparser
import os
import matplotlib.pyplot as plt

from snowflake.snowpark.session import Session
from snowflake.snowpark import functions as F
from snowflake.snowpark.types import *
import pandas as pd
from snowflake.snowpark.functions import udf
import datetime as dt
import numpy as np

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
df = session.sql("SELECT payment_description, total_taxi from DM_payment_count").collect()
# List to dataframe
df = pd.DataFrame(df, columns=['payment_description', 'total_taxi'])
print(df)

# Create a histogram with the description as x axis and total_taxi as y axis
plt.bar(df['payment_description'], df['total_taxi'])

# Customize the plot
plt.title('NYC Taxi Payment Types')
plt.xlabel('Payment Type', color='blue')
plt.ylabel('Total Taxi', color='blue')

# Add the number of the total taxi on the column
for i, v in enumerate(df['total_taxi']):
    plt.text(i, v, str(v), ha='center', va='bottom', fontsize=8)

# Show the plot
plt.show()

# Close the session
session.close()


