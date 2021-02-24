import pandas as pd
import snowflake.connector
from helper.secrets import get_secret_local


def retreive_data_snowflake(sql_qry):

    """

    Run query and return data

    :param qry: SQL query to run
    :return: DataFrame with data fetched
    
    """

    # Secret name and Region
    secretName = "secret_name"
    secretRegion = "secret_region"

    # Get credentials
    user = get_secret(secretName, secretRegion)
    user = json.loads(user)

    snowflake_user = user['user']
    snowflake_pass = user['password']
    snowflake_account = user['account']
    snowflake_warehouse = user['warehouse']
    snowflake_database = user['database']
    snowflake_schema = user['schema']

    # Snowflake connection
    consnow = snowflake.connector.connect(
        user=snowflake_user,
        password=snowflake_pass,
        account=snowflake_account,
        warehouse=snowflake_warehouse,
        database=snowflake_database,
        schema=snowflake_schema,
        session_parameters={
            'QUERY_TAG': 'MPFR'
        }
    )

    # Run query and return data
    try:
        # Gets the version
        cs = consnow.cursor()
        try:
            allrows = cs.execute(sql_qry)
            one_row = cs.fetchall()
        finally:
            cs.close()
        df = pd.DataFrame(one_row)
    except Exception as e:
        print(e)
        df = pd.DataFrame()

    return df
