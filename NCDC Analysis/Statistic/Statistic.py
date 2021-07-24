from pyspark.sql.functions import mean as _mean, stddev as _stddev,count as _count, col, variance as _variance

def mean(df, column_name, start_date, end_date):
    return df.filter((col("DATE") >= start_date) & \
                     (col("DATE") <= end_date)) \
             .select(_mean(column_name).alias(f'{column_name}_MEAN'))

def std(df, column_name, start_date, end_date):
    return df.filter((col("DATE") >= start_date) & \
                     (col("DATE") <= end_date)) \
             .select(_stddev(column_name).alias(f'{column_name}_STD'))

def variance(df, column_name, start_date, end_date):
    return df.filter((col("DATE") >= start_date) & \
                     (col("DATE") <= end_date)) \
             .select(_variance(column_name).alias(f'{column_name}_VARIANCE'))

def median(df, column_name, start_date, end_date):
    df = df.filter((col("DATE") >= start_date) & \
                     (col("DATE") <= end_date)) \
           .approxQuantile(column_name, [0.5],0.01)
    return df[0]

