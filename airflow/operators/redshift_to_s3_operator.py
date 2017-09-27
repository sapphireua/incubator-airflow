# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import os

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class RedshiftToS3Operator(BaseOperator):

    """
    Executes an UNLOAD command to s3 as a CSV with headers
    :param str schema: reference to a specific schema in redshift database
    :param str table: reference to a specific table in redshift database
    :param str s3_bucket: reference to a specific S3 bucket
    :param str s3_key: reference to a specific S3 key
    :param str redshift_conn_id: reference to a specific redshift database
    :param str s3_conn_id: reference to a specific S3 connection
    :param list unload_options: reference to a list of UNLOAD options
    :param bool autocommit: enable or desable autocommit for redshift
    :param str sql: prepared SQL statement for UNLOAD
    :param list column_names: list of header columns in CSV file
    """

    template_fields = ('s3_key',)
    template_ext = ()
    ui_color = '#ededed'

    @apply_defaults
    def __init__(
            self,
            schema,
            table,
            s3_bucket,
            s3_key,
            redshift_conn_id='redshift_default',
            s3_conn_id='s3_default',
            unload_options=None,
            autocommit=False,
            sql=None,
            column_names=None,
            *args, **kwargs):
        super(RedshiftToS3Operator, self).__init__(*args, **kwargs)
        self.schema = schema
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.s3_conn_id = s3_conn_id
        self.redshift_conn_id = redshift_conn_id
        self.unload_options = unload_options
        self.autocommit = autocommit
        self.sql = sql
        self.column_names = column_names
        self.access_key = os.getenv('AWS_ACCESS_KEY_ID')
        self.secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
        self.access_token = os.getenv('AWS_SESSION_TOKEN')

    def execute(self, context):
        """
        Executor which orchestrate the process of transfer data from Redshift database to the specific S3 bucket
        """
        logging.info('Using schema: {}'.format(self.schema))
        pg_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id, schema=self.schema)

        if self.sql:
            logging.info('Preparing custom unload...')
            sql_query = self._make_custom_unload()
        else:
            logging.info('Preparing default unload...')
            sql_query = self._make_default_unload(pg_hook)

        credentials = ("aws_access_key_id={access_key};"
                       "aws_secret_access_key={secret_key}").format(
                           access_key=self.access_key,
                           secret_key=self.secret_key)
        if self.access_token:
            credentials += ";token={access_token}".format(access_token=self.access_token)

        unload_options = '\n\t\t\t'.join(self.unload_options)

        unload_params = {
            'credentials': credentials,
            'sql_query': sql_query,
            'unload_options': unload_options,
            's3_bucket': self.s3_bucket,
            's3_key': self.s3_key,
        }

        unload_query = self._make_unload_query(unload_params)
        logging.info('Unload query is: {}'.format(unload_query))

        logging.info('Executing UNLOAD command...')
        pg_hook.run(unload_query, self.autocommit)
        logging.info("UNLOAD command complete...")

    def _make_default_unload(self, pg_hook):
        """
        Generate SQL part of unload query by which all columns in table will unloaded
        :return str unload_query: formatted query for unload process
        """
        columns_query = """SELECT column_name
                           FROM information_schema.columns
                           WHERE table_schema = '{0}'
                           AND table_name = '{1}'
                           ORDER BY ordinal_position
          """.format(self.schema, self.table)

        logging.info('Retrieve table column names...')
        connection = pg_hook.get_conn()
        cursor = connection.cursor()
        cursor.execute(columns_query)
        rows = cursor.fetchall()
        cursor.close()
        connection.close()

        columns = [row[0] for row in rows]
        column_names = ', '.join(map(lambda c: "\\'{0}\\'".format(c), columns))
        column_castings = ', '.join(map(lambda c: "CAST({0} AS text) AS {0}".format(c), columns))
        unload_query = """SELECT {0}
                          UNION ALL
                          SELECT {1} FROM {2}.{3}
                          """.format(
            column_names,
            column_castings,
            self.schema,
            self.table,
        )
        return unload_query

    def _make_custom_unload(self):
        """
        Generate SQL part of unload query from specific SQL statemen
        :return str sql_query: formatted query for unload process
        """
        column_names = ', '.join(["\\'{0}\\'".format(column) for column in self.column_names])
        sql_query = """SELECT {0} UNION ALL {1}""".format(column_names, self.sql)
        return sql_query

    def _make_unload_query(self, unload_params):
        """
        Generate full prepared UNLOAD query
        :param dict unload_params: dict of parameters for UNLOAD statement
        :return str unload_query: full prepared UNLOAD query
        """

        unload_query = """UNLOAD ('{sql_query}')
                          TO 's3://{s3_bucket}/{s3_key}'
                          with
                          credentials '{credentials}'
                          {unload_options};"""
        return unload_query.format(**unload_params)

