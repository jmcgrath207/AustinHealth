# Austin Health


### Purpose
The Data Analytics team wants all restaurant health inspection scores in the greater Austin, TX area.

The goal is to find out if there is a relationship between hospital admissions 
and restaurant health score that are within a geospatial box of the hospital.

### Requirements

The team will want to query this with a geohash. To
get this, you will have to run the latitude and longitude of restaurant
through a geohash function.

The team will also what to query this by a year, month, and day quickly.

The table can have additional columns as long as the required columns of
geohash, year, month, day exist.

### Implementation

Program needs to be broken up into two tasks. One for fetching data and the other for 
Normalization.

Fetch task:
    - Retrieve JSON from API and store in a buffer. (https://dev.socrata.com/foundry/data.austintexas.gov/ecmv-9xxi)
    - Once buffer is full, convert to a record and push to the data queue.

Normalization task:
    - Wait until record is received from data queue.
    - Convert Record to Dataframe
    - Derive Year, Month, Day from date and make those columns.
    - Create Geohash Column from Longitude and Latitude by running through the geohash function.
    - Once DataFrame is Normalized, dump Dataframe to Data Lake with partion columns being [geohash, year, month, day] 

Considerations:
    - If script fails, it should be able to restart from the last successfully parquet file dump.




