from pyspark import SparkConf, SparkContext
from rich import print
import pyspark.pandas as ps

conf = SparkConf()
conf.setAppName('nyctaxi') # you may want to change this
conf.set('spark.executor.memory', '2g')
conf.set('spark.executor.cores', '1')
conf.set('spark.driver.memory', '10g')
conf.set('spark.driver.cores', '5')
spark = SparkContext(conf=conf)

df = ps.read_parquet('/data/nyctaxi/set1/*.parquet')
# print(df.columns)
# print(df.info(verbose=True))
#
# print(df.groupby('payment_type')['fare_amount'].mean())

# TODO:
# 1. [x] Average ratio of trip cost that is tolls
# 2. [x] Total number of trips per month across all years
# 3. [x] Average price permile, excluding tolls and mta taxes
# 4. [ ] Most popular pickup/dropoff locations (lse lat/long but rounded to 3 decimal places)

print()
print('---------------------------------- Output Starts Here ----------------------------------')

# INFO: Task 1: Average ratio of trip cost that is tolls 
print()
print('[b]Task 1: Average ratio of trip cost that is tolls ')

# Filter out records where total_amount is zero
filtered_df = df[df['total_amount'] > 0]

# Calculate the ratio of tolls to total cost for each trip in the filtered data
filtered_df['tolls_ratio'] = filtered_df['tolls_amount'] / filtered_df['total_amount']

# Calculate the average ratio of trip cost that is tolls in the filtered data
average_tolls_ratio = filtered_df['tolls_ratio'].mean()

# Print the result
print("Average ratio of trip cost that is tolls:", average_tolls_ratio)

# INFO: Task 2: Total number of trips per month across all years
print()
print('[b]Task 2: Total number of trips per month across all years')

# Extract year and month from pickup_datetime
df['year'] = df['pickup_datetime'].dt.year
df['month'] = df['pickup_datetime'].dt.month

# Group by year and month, then count the number of trips
trips_per_month = df.groupby(['year', 'month']).size().rename('total_trips')

# Reset index to make the DataFrame more readable
trips_per_month_reset = trips_per_month.reset_index()

# Sort the DataFrame by year and month for chronological order
trips_per_month_sorted = trips_per_month_reset.sort_values(by=['year', 'month'])

# Print the formatted DataFrame
print(trips_per_month_sorted)

# INFO: Task 3: Average price per mile, excluding tolls and MTA taxes 
print()
print('[b]Task 3: Average price per mile, excluding tolls and MTA taxes ')

# Filter out records where trip_distance is zero to avoid division by zero errors
valid_trips_df = df[df['trip_distance'] > 0]

# Calculate the price excluding tolls and MTA taxes
valid_trips_df['price_excluding_tolls_taxes'] = valid_trips_df['total_amount'] - valid_trips_df['tolls_amount'] - valid_trips_df['mta_tax']

# Calculate the price per mile by dividing the adjusted price by the trip distance
valid_trips_df['price_per_mile'] = valid_trips_df['price_excluding_tolls_taxes'] / valid_trips_df['trip_distance']

# Calculate the average price per mile
average_price_per_mile = valid_trips_df['price_per_mile'].mean()

# Print the result
print("Average price per mile, excluding tolls and MTA taxes:", average_price_per_mile)

# INFO: Task 4: Most popular pickup/dropoff locations (rounded to 3 decimal places)
print()
print('[b]Task 4: Most popular pickup/dropoff locations (rounded to 3 decimal places)')
# Round latitude and longitude to 3 decimal places
df['rounded_pickup_latitude'] = df['pickup_latitude'].round(3)
df['rounded_pickup_longitude'] = df['pickup_longitude'].round(3)
df['rounded_dropoff_latitude'] = df['dropoff_latitude'].round(3)
df['rounded_dropoff_longitude'] = df['dropoff_longitude'].round(3)

# Filter out rows where the rounded latitude and longitude are NaN or (0, 0) for pickups
filtered_pickup = df[(~np.isnan(df['rounded_pickup_latitude']) & ~np.isnan(df['rounded_pickup_longitude'])) & 
                     ((df['rounded_pickup_latitude'] != 0) | (df['rounded_pickup_longitude'] != 0))]

# Filter out rows where the rounded latitude and longitude are NaN or (0, 0) for dropoffs
filtered_dropoff = df[(~np.isnan(df['rounded_dropoff_latitude']) & ~np.isnan(df['rounded_dropoff_longitude'])) & 
                      ((df['rounded_dropoff_latitude'] != 0) | (df['rounded_dropoff_longitude'] != 0))]

# Count occurrences of each rounded latitude and longitude pair for filtered pickups
pickup_locations = filtered_pickup.groupby(['rounded_pickup_latitude', 'rounded_pickup_longitude']).size().reset_index(name='count')
pickup_locations = pickup_locations.sort_values(by='count', ascending=False)

# Count occurrences of each rounded latitude and longitude pair for filtered dropoffs
dropoff_locations = filtered_dropoff.groupby(['rounded_dropoff_latitude', 'rounded_dropoff_longitude']).size().reset_index(name='count')
dropoff_locations = dropoff_locations.sort_values(by='count', ascending=False)

# Display the most popular pickup and dropoff locations
print("Most popular pickup locations:")
print(pickup_locations.head())
print("\nMost popular dropoff locations:")
print(dropoff_locations.head())
