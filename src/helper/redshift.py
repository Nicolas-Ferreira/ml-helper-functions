import pandas as pd
import json
from helper.secrets import get_secret
from helper.secrets import get_secret_local
from sqlalchemy import create_engine
from sqlalchemy import text
import psycopg2


def retreive_data(sql_qry):

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

    redshift_endpoint = user['host']
    redshift_user = user['user']
    redshift_pass = user['pwd']
    port = user['port']
    dbname = user['dbname']

    # Create Redshift conn
    engine_string = "postgresql+psycopg2://%s:%s@%s:%d/%s" \
                    % (redshift_user, redshift_pass, redshift_endpoint, port, dbname)
    engine = create_engine(engine_string)

    # Run query and return data
    try:
        df = pd.read_sql_query(text(sql_qry), engine)
    except:
        df = pd.DataFrame()

    engine.dispose()

    return df


def save_data(df, schema, table):

    """

    Insert data into Redshift Table

    :param df: Dataframe to insert
    :param table: DB table
    :param schema: DB schema
    :return: True or False

    """

    # Secret name and Region
    secretName = "secret_name"
    secretRegion = "secret_region"

    # Get credentials
    user = get_secret(secretName, secretRegion)
    user = json.loads(user)

    redshift_endpoint = user['host']
    redshift_user = user['user']
    redshift_pass = user['pwd']
    port = user['port']
    dbname = user['dbname']

    # Create Redshift conn
    engine_string = "postgresql+psycopg2://%s:%s@%s:%d/%s" \
                    % (redshift_user, redshift_pass, redshift_endpoint, port, dbname)
    engine = create_engine(engine_string)

    # Save to redshift
    try:
        df.to_sql(
            schema=schema,
            name=table,
            con=engine,
            if_exists='append',
            chunksize=100,
            index=False,
            method='multi'
        )
    except:
        return False

    engine.dispose()

    return True


def execute_query(qry):

    """

    Execute query

    :param qry: SQL statement to run
    :return: True or False
    """

    # Secret name and Region
    secretName = "secret_name"
    secretRegion = "secret_region"

    # Get credentials
    user = get_secret(secretName, secretRegion)
    user = json.loads(user)

    redshift_endpoint = user['host']
    redshift_user = user['user']
    redshift_pass = user['pwd']
    port = user['port']
    dbname = user['dbname']

    # Create Redshift conn
    engine_string = "dbname=%s host=%s port=%d user=%s password=%s" \
                    % (dbname, redshift_endpoint, port, redshift_user, redshift_pass)
    conn = psycopg2.connect(engine_string)

    try:
        # Create cursor
        cur = conn.cursor()
        # Execute query
        cur.execute(qry)
        # Commit
        conn.commit()
    except:
        return False

    return True
