# Austin Health


### Purpose
The Data Analytics team wants all restaurant inspection scores in the greater Austin, TX area.

The goal is to find out if there is a relationship between hospital admissions 
and restaurant inspection scores that are within a geospatial box of the hospital.

### Requirements

Ability to query quickly query by geohash, year, month, and day.

If latitude and longitude don't exist, remove those rows.

The table can have additional columns as long as the required columns of
geohash, year, month, day exist.


### Implementation

Program with be coding in python 3.8.7 to leverage the latest asyncio capabilities. 

The program needs to be broken up into two tasks. One for fetching data and the other for 
normalization.

#### Fetch task:
- Retrieve JSON from API and store in a buffer. (https://dev.socrata.com/foundry/data.austintexas.gov/ecmv-9xxi)
- Once buffer is full, convert to a record and push to the data queue.

#### Normalization task:
- Wait until record is received from data queue.
- Convert Record to Dataframe
- Derive Year, Month, Day from date and make those columns.
- Create Geohash Column from Longitude and Latitude by running through the geohash function.
- Once DataFrame is Normalized, dump Dataframe to Data Lake with partition columns being [geohash, year, month, day] 

#### Considerations:
    - If script fails, it should be able to restart from the last successfully parquet file dump.




