#!/usr/bin/env python
# coding: utf-8

# In[1]:


import datetime as dt
from datetime import timedelta
import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

import pandas as pd


# In[2]:


def csv_to_json():
    df = pd.read_csv('/home/zmh93/projects/first_pipeline/data.csv')
    
    for index, row in df.iterrows():
        print(row['name'])
    
    df.to_json('fromAirflow.json',orient='records')


# In[3]:


# set up default arguments
default_args = {
    'owner':'zawmyohtet',
    'start_date':dt.datetime(2023, 4, 12),
    'retries':1,
    'retry_delay':dt.timedelta(minutes=5),
}


# In[4]:


### DAG Definition ###

#define the DAG
dag = DAG(
    dag_id = 'csv-to-json-dag',
    default_args = default_args,
    description = 'Transform CSV to JSON',
    schedule = timedelta(days=1), #tells us how frequently this DAG runs. In this case every day.
)


# In[5]:


dag.default_args['start_date']


# In[6]:


# define the tasks

print_starting = BashOperator(task_id='starting',
                             bash_command='echo "I am reading the CSV now..."',
                             dag=dag)

CSVtoJSON = PythonOperator(task_id='convert_csv_to_json',
                            python_callable=csv_to_json,
                          dag=dag)


# In[7]:


# connect the tasks using bit shift operator
print_starting >> CSVtoJSON


# In[ ]:




