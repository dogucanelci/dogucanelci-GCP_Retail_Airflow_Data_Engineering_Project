from airflow import DAG
from datetime import datetime
from airflow.operators.python_operator import PythonOperator

def func1(a):
    return a * a

def func2(a):
    return a * 2

def func3(a, b):
    return a + b

def func4(a):
    return a + 10

with DAG(
    dag_id='example1',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:
    
    a = 2
    b = 3

    f1 = func1(a)
    f2 = func2(b)
    f3 = func3(f1, f2)
    f4 = func4(f3)

    # Taskleri birbirine bağla
    t1 = PythonOperator(
        task_id='task_func1',
        python_callable=func1,
        op_kwargs={'a': a},
        dag=dag,
    )

    t2 = PythonOperator(
        task_id='task_func2',
        python_callable=func2,
        op_kwargs={'a': b},
        dag=dag,
    )

    t3 = PythonOperator(
        task_id='task_func3',
        python_callable=func3,
        op_kwargs={'a': f1, 'b': f2},
        dag=dag,
    )

    t4 = PythonOperator(
        task_id='task_func4',
        python_callable=func4,
        op_kwargs={'a': f3},
        dag=dag,
    )

    [t1,t2] >> t3 >> t4