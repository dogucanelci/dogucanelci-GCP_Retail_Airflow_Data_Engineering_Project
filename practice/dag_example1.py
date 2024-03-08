from airflow import DAG
from datetime import datetime

def func1(a):
    return a*a

def func2(a):
    return a*2

def func3(a,b):
    return a +b

def func4(a):
    return a + 10

with DAG(
dag_id='example1',
         start_date=datetime(2023, 1, 1),
         schedule_interval=None,
         catchup=False,) as dag:
    
    f1 = func1(2)
    f3 = func3(f1,f2)
    f2 = func2(3)
    f4 = func4(f3)

[f1,f2] >> f3 >> f4

