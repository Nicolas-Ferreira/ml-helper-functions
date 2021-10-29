from pyspark.sql import functions as F
from pyspark.sql.window import Window


def calculate_time_window_indicator(
        df,
        column_name,
        time_window,
        group_column,
        filter_criteria,
        agg_column = 'sum_column',
        timestamp_column
    ):

    """
    Function to calculate count aggregation grouped by a column and using provided time window and filter criteria

    :param df: spark dataframe with data to aggregate
    :param column_name: new column name
    :param time_window: time window to aggregate values
    :param group_column: column used to group by
    :param filter_criteria: filter criteria to apply before aggregation
    :param timestamp_column: column with timestamp to use for aggregation
    :return df: spark dataframe with aggregated column
    """

    # Time window dic
    dict_time_window = {
        '60M': 60,
        '2H': 2,
        '3H': 3,
        '12H': 12,
        '24H': 24,
        '3D': 3,
        '7D': 7,
        '15D': 15,
        '30D': 30,
        '60D': 60,
        '90D': 90,
        '365D': 365,
    }

    # Check if time window is correct
    if not time_window in dict_time_window:
        print('ERR PARAM: invalid time_window')
        return df

    # Check if column_name is correct
    if column_name in df.columns:
        print('ERR PARAM: invalid column_name, already exists on dataframe')
        return df

    # Check if group_column is correct
    if group_column not in df.columns:
        if not all(item in df.columns for item in group_column):
            print('ERR PARAM: invalid time_window')
            return df

    # Lambdas to calcule time in seconds
    days = lambda i: i * 86400
    hours = lambda i: i * 3600
    minutes = lambda i: i * 60

    # Create time window with paramenter
    if 'M' in time_window:
        window_parameter = minutes(dict_time_window[time_window])
    elif 'H' in time_window:
        window_parameter = hours(dict_time_window[time_window])
    elif 'D' in time_window:
        window_parameter = days(dict_time_window[time_window])

    # Define time window with partition
    windowSpec = Window.partitionBy(
        group_column
    ).orderBy(
        F.col(timestamp_column).cast('long')
    ).rangeBetween(-window_parameter, 0)

    # Add temp column used for aggregation that meet criteria
    if agg_column == 'sum_column':
        df = df.withColumn(
            agg_column,
            F.when(
                (filter_criteria),
                1
            ).otherwise(0)
        )
    else:
        df = df.withColumn(
            agg_column,
            F.when(
                (filter_criteria),
                df[agg_column]
            ).otherwise(0)
        )

    # Calculate rolling on provided window
    df = df.withColumn(
        column_name,
        F.sum(
            agg_column
        ).over(
            windowSpec
        )
    )

    # Substract one index due to start with 1
    if agg_column == 'sum_column':
        df = df.withColumn(
            column_name,
            F.when(
                (df.sum_column == 1) & (df[column_name] > 0),
                df[column_name] - 1
            ).otherwise(df[column_name])
        )

    # Drop temporary columns
    df = df.drop(*['sum_column'])

    return df
