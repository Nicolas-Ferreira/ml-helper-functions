import s3fs
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.exceptions import ClientError
import logging
from helper.secrets import get_secret_local


def write_df_to_s3_as_parquet(df, outdir):

    """

    Function to export DataFrame as Parquet on S3

    :param df: dataframe with the data ready to write
    :param outdir: path with file name to write: ei: 'datalake-financiera-rawdata/npos_nxd/mod_churn/leads_totales'
    :return: True or False

    """
    
    fs = s3fs.S3FileSystem()

    table = pa.Table.from_pandas(df)

    try:
        pq.write_to_dataset(table=table,
                            root_path='s3://' + outdir,
                            filesystem=fs)
    except ClientError as e:
        logging.error(e)
        return False

    return True
