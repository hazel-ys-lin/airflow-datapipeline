"""
  Operator to load data
  from postgres to csv
"""

# from datetime import datetime, timedelta
# import os
# import io
import csv

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataSourceToCsvOperator(BaseOperator):
    """
      Extract data from the data source to CSV file
    """

    # template_fields = ('sql', )
    # template_ext = ('.sql', )
    # ui_color = '#ededed'

    @apply_defaults
    def __init__(
            self,
            # sql: str,
            postgres_conn_id='uvs_postgres_conn',
            #  autocommit=False,
            #  parameters=None,
            database=None,
            *args,
            **kwargs):
        super().__init__(*args, **kwargs)
        # self.sql = sql
        self.postgres_conn_id = postgres_conn_id
        # self.autocommit = autocommit
        # self.parameters = parameters
        self.database = database
        self.file_path = '/tmp/'

    def execute(self, context):
        from psycopg2.extras import copy_to
        import psycopg2

        query = "SELECT * FROM user_org;"
        self.log.info('Executing: %s', query)
        self.hook = PostgresHook(postgres_conn_id=self.postgres_conn_id, schema=self.database)

        conn = self.hook.get_conn()
        cursor = conn.cursor(query)
        # self.hook.run(self.sql, self.autocommit, parameters=self.parameters)
        filename = "dump.csv"
        with open(filename, "w", encoding="utf8") as f:
            copy_to(cursor, "user_org", f, sep=",", null="")
        cursor.close()
        conn.close()

        # cursor.execute(self.sql)
        # result = cursor.fetchall()

        # # Write to CSV file
        # temp_path = self.file_path + '_dump_.csv'
        # tmp_path = self.file_path + 'dump.csv'
        # with open(temp_path, 'w') as fp:
        #     a = csv.writer(fp, quoting=csv.QUOTE_MINIMAL, delimiter='|')
        #     a.writerow([i[0] for i in cursor.description])
        #     a.writerows(result)
        # # full_path = temp_path + '.gz'
        # with open(temp_path, 'rb') as f:
        #     data = f.read()
        # f.close()
        # self.hook.bulk_dump(self.sql, tmp_path)
