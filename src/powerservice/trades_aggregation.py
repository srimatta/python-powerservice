import sys
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F


def get_schema():
    """ creating schema for the input data """
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

    schema = StructType([ \
        StructField('date', StringType(), True), \
        StructField('time', StringType(), False), \
        StructField('volume', FloatType(), True), \
        StructField('id', StringType(), True)
    ])
    return schema


def get_trades_local_date_time_df(spark, schema, input_trades_csv_path, timezone='Etc/GMT-1') -> DataFrame:
    trades_df = spark.read.schema(schema).option('header', True).csv(input_trades_csv_path)

    trades_date_time_df = trades_df.withColumn('date_time',
                                               F.to_timestamp(F.concat_ws(' ', 'date', 'time'), input_date_format)). \
        filter('date_time is not null')

    '''converting time to utc time format with adjusting with local time zone'''
    hr_format = '%02d:00'
    trades_local_date_time_df = trades_date_time_df.withColumn('date_time_utc',
                                                               F.to_utc_timestamp('date_time', timezone)). \
        withColumn("LocalTime", F.format_string(hr_format, F.hour('date_time_utc'))). \
        withColumn('LocaDate', F.to_date('date_time_utc'))

    return trades_local_date_time_df


def get_aggr_data(trades_local_date_time_df):

    """Aggregated volume based on LocalTime. Spark runs on multiple nodes in parallel so ordering is not guarantee.
      As dataset here is small coalesce is set to '1' so all the records moved to one node, Ordering will happen as
      expected """

    trades_aggregated = trades_local_date_time_df.coalesce(1).groupBy('LocaDate', 'LocalTime').sum('volume').orderBy(
        'LocaDate'). \
        select(F.col('LocalTime'), F.col('sum(volume)').alias('Volume'))
    return trades_aggregated


def get_output_filename(df):
    filename_date_time_format = '%Y%m%d_%H%M'
    latest_date_time = df.select(F.max('date_time_utc')).first()['max(date_time_utc)']
    latest_date_time = latest_date_time.strftime(filename_date_time_format)
    output_filename = "PowerPosition_{date_time}.csv".format(date_time=latest_date_time)
    return output_filename


if __name__ == '__main__':
    # Create SparkSession
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("powerservice") \
        .getOrCreate()

    input_trades_csv_path = sys.argv[1]  # trades_data/trades.csv
    input_date_format = sys.argv[2]      # "dd/MM/yyy HH:mm"
    print('input csv file path : ', input_trades_csv_path)
    print('date format : ', input_date_format)

    schema = get_schema()
    trades_local_date_time_df = get_trades_local_date_time_df(spark, schema, input_trades_csv_path)

    trades_aggregated = get_aggr_data(trades_local_date_time_df)
    trades_aggregated.show(trades_aggregated.count())

    '''Saving and reading output file'''
    output_filename = get_output_filename(trades_local_date_time_df)
    print('output file path : ', output_filename)
    trades_aggregated.coalesce(1).write.mode('overwrite').option("header", True).csv(output_filename)

