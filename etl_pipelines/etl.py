__version__ = '1.0.0'

import io
import re
import os
import bz2
import json
import pickle
import hashlib
import logging
import calendar
import sqlalchemy
import looker_sdk
import configparser
import pandas as pd
import _pickle as c_pickle

from typing import List, Callable
from pg8000 import dbapi
from logging import Logger
from datetime import datetime, date
from matterhook import Webhook
from looker_sdk import models40
from google.cloud import storage
from google.cloud import bigquery
from dateutil import relativedelta
from sqlalchemy.engine import Engine
from google.cloud import secretmanager
from pandas.errors import EmptyDataError
from dataclasses import dataclass, field
from google.cloud.sql.connector import Connector
from office365.sharepoint.files.file import File
from google.cloud.bigquery import Client as bq_client
from google.cloud.storage.client import Client as gcs_client
from google.cloud.bigquery import SchemaField, LoadJobConfig
from office365.sharepoint.client_context import ClientContext
from office365.runtime.auth.authentication_context import AuthenticationContext

# pandas setup
pd.set_option('max_rows', 500)
pd.set_option('max_columns', 50)


class LoggerOps:
    @staticmethod
    def init_logger(file_log_name: str = None) -> Logger:
        """
        Set up and format both stream (console) and file logging processes

        :param file_log_name: the log file's name
        """
        logger = logging.getLogger()

        # Create handlers
        stream_handler = logging.StreamHandler()

        # Setting logger to the lowest level and then handlers individually
        logger.setLevel(logging.DEBUG)
        stream_handler.setLevel(logging.INFO)

        # Create formatters and add it to handlers
        format_new = "{asctime} |{levelname: ^8} - {module: ^15} - {funcName: ^15} - line:{lineno: <3} |" \
                     " --> {message}"
        format_log = logging.Formatter(format_new, style='{')

        # Add formatter
        stream_handler.setFormatter(format_log)

        # Add handlers to the logger
        if not logger.handlers:
            logger.addHandler(stream_handler)

        # If a file log is desired
        if file_log_name:
            # Create handlers
            file_handler_debug = logging.FileHandler(file_log_name, mode='w')
            # Set level
            file_handler_debug.setLevel(logging.DEBUG)
            # Add formatter
            file_handler_debug.setFormatter(format_log)
            # Add handler
            if not logger.handlers:
                logger.addHandler(file_handler_debug)

        return logger


# Set up a logger
logger = LoggerOps.init_logger()


@dataclass
class Auth:
    # The location of the local google service account .json file (notebook usage)
    gauth_filename: str = None
    looker_sdk_client: looker_sdk.methods40.Looker40SDK = field(init=False, default=None)
    gcp_client: gcs_client = field(init=False, default=None)
    bq_client: bq_client = field(init=False, default=None)

    @staticmethod
    def get_secret(project: str, secret_id: str) -> str:
        """A small function that retrieves secrets from Google secrets"""
        client = secretmanager.SecretManagerServiceClient()

        # Build the resource name of the secret version.
        name = f"projects/{project}/secrets/{secret_id}/versions/latest"

        # Access the secret version.
        response = client.access_secret_version(name=name)

        # Return the decoded payload.
        return response.payload.data.decode('UTF-8')

    def __post_init__(self):
        """
        - Makes sure your environment is set to authenticate to GCP
        - Connect to Looker, GCP and BQ.
         """
        # This points to the service account .json file that authenticates to use GCP
        # You can always set this as an environmental variable in your IDE
        try:
            assert type(os.environ['GOOGLE_APPLICATION_CREDENTIALS']) == str
        except KeyError:
            if self.gauth_filename is not None:
                os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = self.gauth_filename
            else:
                raise ValueError('Please insert valid GCP credentials')

        # Below a connection to Looker is being created through Google secrets
        config = configparser.ConfigParser()
        config.read_string(self.get_secret(project='fake_project',
                                           secret_id="fake_looker_secret"))
        os.environ['LOOKERSDK_CLIENT_ID'] = config['Looker']['client_id']
        os.environ['LOOKERSDK_CLIENT_SECRET'] = config['Looker']['client_secret']
        os.environ['LOOKERSDK_BASE_URL'] = config['Looker']['base_url']
        self.looker_sdk_client = looker_sdk.init40()

        # Create a GCP storage client
        self.gcp_client = storage.Client()

        # Create a BQ storage client
        self.bq_client = bigquery.Client()

@dataclass
class SharepointOps(Auth):
    def sharepoint_folder_auth(self, secret_id: str, folder_url: str) -> ClientContext:
        """Enable the connection to a sharepoint folder"""
        ctx_auth = AuthenticationContext(folder_url)
        secret = json.loads(self.get_secret(project='fake_project',
                                            secret_id=secret_id))
        ctx_auth.acquire_token_for_app(client_id=secret['client_id'],
                                       client_secret=secret['client_secret'])
        ctx = ClientContext(folder_url, ctx_auth)
        web = ctx.web
        ctx.load(web)
        # Test connection
        ctx.execute_query()

        return ctx

    @staticmethod
    def read_sharepoint_excel(ctx: ClientContext, file_path: str, sheet_name:str) -> pd.DataFrame:
        """
        Read excel file from sharepoint folder

        :param ctx: the ClientContext to access specific sharepoint folder
        :param file_path: the file path in Sharepoint
        :param sheet_name: the sheet name to transform to dataframe
        """
        response = File.open_binary(ctx, file_path)
        bytes_file_obj = io.BytesIO()
        bytes_file_obj.write(response.content)
        bytes_file_obj.seek(0)

        df = pd.read_excel(bytes_file_obj, sheet_name, engine='openpyxl', header=None)

        return df


class FileOps:
    @staticmethod
    def compressed_pickle(title, data):
        """
        Pickle a file and then compress it into a file with extension
            Pickle compression and decompression pipeline credit:
            https://betterprogramming.pub/load-fast-load-big-with-compressed-pickles-5f311584507e
        """
        with bz2.BZ2File(title, 'w') as f:
            c_pickle.dump(data, f)

    @staticmethod
    def decompress_pickle(file):
        """
        Load any compressed pickle file
            Pickle compression and decompression pipeline credit:
            https://betterprogramming.pub/load-fast-load-big-with-compressed-pickles-5f311584507e
        """
        data = bz2.BZ2File(file, 'rb')
        data = c_pickle.load(data)
        return data

    @staticmethod
    def save_pickle(serializable_obj, filepath):
        with open(filepath, 'wb') as f:
            pickle.dump(serializable_obj, f)

    @staticmethod
    def load_pickle(filepath):
        with open(filepath, 'rb') as f:
            return pickle.load(f)


class LookerOps(Auth):
    def execute_query_csv(self, sql: str, model_name: str) -> pd.DataFrame:
        """
        Function to execute a query in Looker.
            Model_name needs to be changed depending on the model you are querying.
            You can check which model to pick in SQL Runner

        :param sql: The query we want to run
        :param model_name: the model name for the connection section in SQL runner
        """
        slug = self.looker_sdk_client.create_sql_query(body=models40.SqlQueryCreate(model_name=model_name,
                                                                                    sql=sql)).slug

        result = self.looker_sdk_client.run_sql_query(slug=slug,
                                                      result_format="csv",
                                                      transport_options={"timeout": 300})

        try:
            df = pd.read_csv(
                io.StringIO(result),
                sep=',',
                engine='python'
            )
            return df
        except EmptyDataError:
            logger.info('No data to return')

    def get_looker_look(self, look_id: int, limit: int = 100_000) -> pd.DataFrame:
        """
        A function to retrieve the data from a Looker look.

        :param look_id: The Looker look id you are targeting
        :param limit: Set's the limit of the look's rows.
                -1 used to be able to allow unlimited download but not anymore.
                So we use a big number instead (<200_000 in my tests)
    https://community.looker.com/general-looker-administration-35/legacy-feature-allowing-unlimited-downloads-looker-4-14-4987
        """
        _ = self.looker_sdk_client.me()
        look = self.looker_sdk_client.look(look_id=look_id)
        response = self.looker_sdk_client.run_look(look_id=look.id, result_format="json", limit=limit)
        df = pd.json_normalize(json.loads(response))

        return df


class SQLOps(Auth):
    @staticmethod
    def create_sql_delete_insert(table: str, data: pd.DataFrame) -> str:
        """
        A function that:
          -> turns the contents of a pandas dataframe to string and
          -> creates the necessary sql query to insert them in a table row-wise

        * In all cases the destination table needs to be created before importing data.
        e.g. execute similar code in SQL Runner:
        CREATE TABLE test_table ( id INTEGER ... TEXT ... NUMERIC )

        ** You can modify the simple INSERT sql template query to anything you would like.
        e.g. You could add a REMOVE statement before the INSERT one.

        :param table: The table name to insert the rows to
        :param data: The dataframe that we need to insert to our DB
        """
        cols = ', '.join(data.columns.to_list())
        vals = []

        for index, r in data.iterrows():
            row = []
            for x in r:
                row.append(f"'{str(x)}'")

            row_str = ', '.join(row)
            vals.append(row_str)

        f_values = []
        for v in vals:
            f_values.append(f'({v})')

        f_values = ', '.join(f_values)
        f_values = re.sub(r"('None')", "NULL", f_values)

        sql = f"DELETE FROM {table};" \
              f"INSERT INTO {table} ({cols}) values {f_values};"

        return sql

    def get_pg_engine(self, db_description: dict) -> Engine:
        """
        Returns a PostgreSQL engine
        :param db_description:
        """
        connector = Connector()
        secrets = json.loads(self.get_secret(secret_id=db_description['secret'],
                                             project='fake_project'))

        def getconn() -> dbapi.Connection:
            conn: dbapi.Connection = connector.connect(
                db_description['server'],
                "pg8000",
                user=secrets['user'],
                password=secrets['password'],
                db=db_description['db']
            )
            return conn

        engine = sqlalchemy.create_engine(
            "postgresql+pg8000://",
            creator=getconn
        )
        engine.dialect.description_encoding = None
        return engine

    def execute_sql_alchemy(self,
                            specifications: dict,
                            df: pd.DataFrame,
                            table_name: str,
                            model_name: str,
                            dtypes: dict) -> None:
        """
        Transform a dataframe to a PostgreSQL table

        :param specifications: dict including server, db and secret keys/values
        :param df: the dataframe to be transformed to table
        :param table_name: the postgresql table name
        :param model_name: the Postgresql model name
        :param dtypes: the types of the dataframe columns. structure like this:
                        {'time': sqlalchemy.types.TIMESTAMP(),
                        'id': sqlalchemy.types.Integer(),
                        'names_list': sqlalchemy.types.ARRAY(String),
                        'values': sqlalchemy.types.Float()}
        """
        if df.empty:
            logger.info('DF empty, no update')
            return

        engine = self.get_pg_engine(specifications)

        logger.info(f'Started filling the table {model_name}.{table_name}')
        with engine.connect() as con:
            _ = df.to_sql(table_name, con, index=False, schema=model_name,
                          if_exists='append', method='multi', chunksize=800,
                          dtype=dtypes)
        engine.dispose()
        logger.info(f'Completed filling the table {model_name}.{table_name}')

class GCPOps(Auth):
    def get_gcp_storage_blob(self, filename: str, bucket_str: str) -> pd.DataFrame:
        """
        A function to retrieve files from gcp storage.

        :param filename: The filename(path) you want to retrieve
        :param bucket_str: The bucket name you are trying to access
        """
        # Initiate a storage client
        bucket = self.gcp_client.bucket(bucket_str)
        blob = bucket.blob(filename)

        # If csv file
        data = blob.download_as_string()
        df_blob = pd.read_csv(io.BytesIO(data))

        return df_blob

    def get_gcp_storage_blob_file(self, filename: str, bucket_str: str, save_path: str) -> None:
        """
        A function to retrieve files from gcp storage.

        :param filename: The filename(path) you want to retrieve
        :param bucket_str: The bucket name you are trying to access
        :param save_path: The save path if filename
        """
        # Initiate a storage client
        bucket = self.gcp_client.bucket(bucket_str)
        blob = bucket.blob(filename)

        _ = blob.download_to_filename(save_path)

    def upload_to_gcp(self, filename: str, df: pd.DataFrame, bucket_str: str) -> None:
        """
        A function to upload a dataframe to gcp storage.

        :param filename: The filename(path) you want to retrieve
        :param df: The targeted dataframe
        :param bucket_str: The GCP storage bucket to access
        """
        bucket = self.gcp_client.bucket(bucket_str)

        bucket.blob(filename).upload_from_string(df.to_csv(index=False), 'text/csv')

        logger.info(f"Uploaded {bucket_str}/{filename}")

    def upload_to_gcp_file(self, filename: str, local_path_filename: str, bucket_str: str) -> None:
        """
        A function to upload a dataframe to gcp storage.

        :param filename: The filename(path) you want to retrieve
        :param local_path_filename: the local filename of the file to be uploaded
        :param bucket_str: The GCP storage bucket to access
        """
        bucket = self.gcp_client.bucket(bucket_str)

        # If local filename
        bucket.blob(filename).upload_from_filename(local_path_filename)

        logger.info(f"Uploaded {bucket_str}/{filename}")


@dataclass
class MattermostOps:
    webhook_url: str = None
    api_key: str = None
    user_name: str = 'The Messenger Bot'

    def send_mm_message(self, message) -> None:
        """
        Send a message to Mattermost channel, using the issued credentials

        Create the credentials by:
        a) clicking on the channel you want to send a message
        b) click on the top left grid-like square
        c) click integrations
        d) create an incoming webhook
        """
        # Create Webhook object
        mwh = Webhook(self.webhook_url, self.api_key)

        # Bot name
        mwh.username = self.user_name

        # Send a message to the desired channel
        return mwh.send(message)


class BigQueryOps(Auth):
    def write_truncate_bq_table(self,
                                df: pd.DataFrame,
                                table_id: str,
                                bq_schema: List[SchemaField]) -> None:
        """
        Overwrites a Big Query table

        :param df: the dataframe to transform to BQ table
        :param table_id: the full table id of the destination table
        :param bq_schema: expecting the dataframe columns and their respective types
                          in the following structure:
                            SchemaField("names", "STRING"),
                            SchemaField("dates", "DATE"),
                            SchemaField("values", "INT64"),
        """
        job_config = LoadJobConfig(
            schema=bq_schema,
            write_disposition="WRITE_TRUNCATE",
        )
        job = self.bq_client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()


@dataclass
class DatetimeOps:
    current_date: datetime = datetime.today()

    @staticmethod
    def date_info(df_original: pd.DataFrame,
                  date_column: str = None) -> pd.DataFrame:
        """
        Add some datetime information, like name of day, week etch, to the beginning of
        a dataframe

        :param df_original: the dataframe to be transformed
        :param date_column: the column holding the datetime info
        """
        df = df_original.copy()
        if date_column is not None:
            df[date_column] = pd.to_datetime(df[date_column])
            if 'name' not in df.columns:
                df.insert(loc=1, column='name', value=[date.strftime("%A") for date in df[date_column]])
            if 'month' not in df.columns:
                df.insert(loc=1, column='month', value=df[date_column].dt.month)
            if 'week' not in df.columns:
                df.insert(loc=1, column='week', value=df[date_column].dt.to_period('W-SUN'))
            if 'week_num' not in df.columns:
                df.insert(loc=1, column='week_num', value=[date.isocalendar()[1] for date in df[date_column]])
            if 'year' not in df.columns:
                df.insert(loc=1, column='year', value=df[date_column].dt.year)

        return df

    def date_delta(self, month_delta: int) -> datetime:
        """
        Get the date for month_delta months away

        :param month_delta: positive or negative months ahead of current month
        """
        return self.current_date + relativedelta.relativedelta(months=month_delta)

    def month(self, month_delta: int) -> int:
        """
        Get the month for month_delta months away

        :param month_delta: positive or negative months ahead of current month
        """
        return (self.current_date + relativedelta.relativedelta(months=month_delta)).month

    def year(self, month_delta: int) -> int:
        """
        Get the year for month_delta months away

        :param month_delta: positive or negative months ahead of current month
        """
        return (self.current_date + relativedelta.relativedelta(months=month_delta)).year

    def month_dates(self, month_delta: int = None, full_date: datetime = None) -> pd.Series:
        """
        Get a pd.Series of the dates for a month month_delta months away

        :param month_delta: positive or negative months ahead of current month
        :param full_date: a specific date to get its full month's dates from
        """
        if full_date is None:
            full_date = self.current_date + relativedelta.relativedelta(months=month_delta)
        number_of_days = calendar.monthrange(full_date.year, full_date.month)[1]

        return pd.Series([date(full_date.year, full_date.month, day)
                          for day in range(1, number_of_days + 1)])


class DataframeOps:
    @staticmethod
    def rename_columns_lowercase_underscores(df):
        df = df.columns.str.lower().str.replace('.', '_')
        return df

    @staticmethod
    def hash_columns(df: pd.DataFrame, cols: List[str] = None) -> pd.DataFrame:
        """
        Create α hash key for the combination of the every row of the dataframe

        :param df: the dataframe to be transformed
        :param cols: the list of columns to include in the hash
        """
        hashed_df = df.copy()
        # Create a hash key for come columns
        if cols is None:
            cols = df.columns
        hashed_df["hash_keys"] = hashed_df[cols] \
            .astype(str) \
            .sum(axis=1) \
            .apply(lambda x: hashlib.sha1(str(x).encode('utf-8')).hexdigest())

        return hashed_df


@dataclass
class AlertingOps:
    mm_ops: MattermostOps = None
    message: Callable = None

    def exception_handler(self, func):
        def inner_function(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception:
                self.mm_ops.send_mm_message(message=self.message(func))

        return inner_function
