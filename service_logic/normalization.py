import json
from datetime import datetime

import pyarrow
import pyarrow.parquet as parquet
import pandas
from pandas import DataFrame

from service_logic import app, set_current_offset
from service_logic.record import Record
import pygeohash


def pandas_transform(df: DataFrame) -> DataFrame:
    """
    Normalized Dataframe columns with JSON dictionaries into
    there own column. Extract Year, month, and Day from inspection_date column.
    Then create Geohash column from latitude and longitude.

    :param df: DataFrame
    :return: df: DataFrame
    """

    # Explode python dicts in address column to their own columns
    df2: DataFrame = pandas.json_normalize(df['address'])
    df.drop(columns=['address'], inplace=True)

    # Convert json to python dicts in human_address column,
    # then exploding them to their own columns
    df3: DataFrame = pandas.json_normalize(df2['human_address'].map(lambda x: json.loads(x)))
    df2.drop(columns=['human_address'], inplace=True)

    # Join all new Dataframes by their indices into one new Dataframe
    # https://stackoverflow.com/a/36539295/3263650
    df: DataFrame = df.merge(df2, how='outer', left_index=True, right_index=True)
    df: DataFrame = df.merge(df3, how='outer', left_index=True, right_index=True)
    del df2
    del df3
    df.dropna(subset=['latitude', 'longitude'], inplace=True)

    # Convert inspection_date to datetime object, then derive a year month day column from it.
    df['inspection_date'] = df['inspection_date'].map(datetime.fromisoformat)
    df[["year", "month", "day"]] = df.apply(
        lambda x: [x['inspection_date'].year, x['inspection_date'].month, x['inspection_date'].day],
        axis=1, result_type="expand")
    df.drop(columns=['inspection_date'], inplace=True)

    # convert latitude and longitude to floats, then
    # combine those columns and apply the geohash function
    # to create the geohash column
    # https://stackoverflow.com/a/52854800/3263650
    df['latitude'] = df['latitude'].map(float)
    df['longitude'] = df['longitude'].map(float)
    df['geohash'] = df[['latitude', 'longitude']].apply(
        lambda x: pygeohash.encode(latitude=x[0], longitude=x[1], precision=12), axis=1)

    return df


async def normalization() -> None:
    """
    Waits until Record is available, dump record to parquet format,
    then checkpoint last successful offset

    This function runs as a separate task in the asyncio event loop

    :return: None
    """
    while True:
        record: Record = await app.data_queue.get()

        df: DataFrame = pandas.DataFrame.from_records(record.payload)

        df: DataFrame = pandas_transform(df=df)

        table = pyarrow.Table.from_pandas(df)

        parquet.write_to_dataset(table=table, root_path=str(app.parquet_path.joinpath('restaurant_inspections')),
                                 compression='snappy',
                                 partition_cols=['geohash', 'year', 'month', 'day']
                                 )

        await set_current_offset(offset=record.checkpoint_offset)
