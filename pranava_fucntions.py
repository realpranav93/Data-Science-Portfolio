from commonly_used_libraries import *
from email_config import *


def get_sqlalchemy_engine():
    engine_query = (f"""postgresql+psycopg2://XXXXXXXXX""")
    engine = create_engine(engine_query, echo = False)
    return engine

def write_to_redshift(table_name,  dataframe,  engine, schema):
    dataframe.to_sql(table_name,  engine,  if_exists = 'append',  chunksize = 1000,  index = False)

def __get_data(query, conn=oms_conn):
    '''Executes query using db connection provided which defaults to OMS'''
    df = sqlio.read_sql_query(query, conn)
    return df

def export_csv(df):
    with io.StringIO() as buffer:
        df.to_csv(buffer, index = False)
        return buffer.getvalue()

def fix_comment(row):
  if row['comment'] is not None:
    #if len(row['comment']) > 200:
    #  print(row['comment'])
    return row['comment'][0:100]
  else:
    return row['comment']

def send_dataframes(subject, body, needed_dataframes):
    context = ssl.create_default_context()
    multipart = MIMEMultipart()
    multipart['From'] = sender_email
    multipart['To'] = receiver_email
    multipart['Subject'] = subject
    for filename in needed_dataframes:
        attachment = MIMEApplication(export_csv(needed_dataframes[filename]))
        attachment['Content-Disposition'] = 'attachment; filename="{}"'.format(filename)
        multipart.attach(attachment)
    multipart.attach(MIMEText(body, 'html'))
    s = smtplib.SMTP_SSL(smtp_server, port, context=context)
    s.login(sender_email, password)
    s.sendmail(sender_email, receiver_email, multipart.as_string())
    s.quit()

def pivot(df, columns_to_keep, column_to_pivot):
    needed_sub_tables = list(set(df['week_start']))
    print(needed_sub_tables)
    merge_tables = []
    for week in needed_sub_tables:
        temp_df = df.copy()
        subset_df = temp_df.query("week_start == @week")
        week = week.split(" ")[0]
        subset_df = subset_df[columns_to_keep]
        subset_df.columns = [ column_name+"_"+week.replace("-","_") for column_name in subset_df.columns ]
        rename_string = 'driver_id'+"_"+week.replace("-","_")
        subset_df.rename(columns = {rename_string:'driver_id'}, inplace = True)
        merge_tables.append(subset_df)
    df_merged = reduce(lambda  left,right: pd.merge(left,right,on=['driver_id'],
                                            how='inner'), merge_tables)
    print(df_merged.head(10))

redshift_engine = get_sqlalchemy_engine()

import pandas as pd
from pandas.io.sql import SQLTable
import sqlalchemy
import psycopg2
import pandas as pd, numpy as np
def _execute_insert(self, conn, keys, data_iter):
   print("Using monkey-patched _execute_insert")
   data = [dict(zip(keys, row)) for row in data_iter]
   conn.execute(self.table.insert().values(data))
SQLTable._execute_insert = _execute_insert



from sqlalchemy import create_engine
def get_sqlalchemy_engine():
   engine_query = (f"""postgresql+psycopg2://XXXXXX""")
   engine = create_engine(engine_query, echo = False)
   return engine
def write_to_redshift(table_name,  dataframe,  engine, schema):
   dataframe.to_sql(table_name,  engine,  if_exists = 'append',  chunksize = 1000,  index = False)
redshift_engine = get_sqlalchemy_engine()

print('functions created')

from os import listdir
csv_list = listdir('/Users/porter-user/Documents/Analytics/porter gold/Porter_tracker_csv/')
for csv in csv_list:
    if '.csv' in csv:
        table = csv.replace('.csv','').lower()
        current_data = pd.read_csv('/Users/porter-user/Documents/Analytics/porter gold/Porter_tracker_csv/'+ table +'.csv')
        write_to_redshift(table,  current_data,  redshift_engine, "anant")
        print(table + ' data uploaded')

print('csv created')


