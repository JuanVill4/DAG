
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator      
from airflow.operators.bash_operator import BashOperator
 
import numpy as np
import math 
from datetime import datetime

vector = [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30 ]

def _primer_metodo(vector):
    v = np.array(vector)
    magnitude = np.linalg.norm(vector)
    return magnitude

def _segundo_metodo(vector):
    magnitude = math.sqrt(sum([x**2 for x in vector]))
    return magnitude

def _tercer_metodo(vector):
    sum_of_squares = 0
    for element in vector:
        sum_of_squares += element**2
    magnitude = math.sqrt(sum_of_squares)
    return magnitude

# define una función que escoja el mejor modelo por su tiempo de ejecucion
def choose_best_model(ti):
    times = ti.xcom_pull(task_ids=["primer_metodo", "segundo_metodo", "tercer_metodo"])
    best_time = min(times)
    if (best_time < 0.0001 ):
        return 'fast'
    elif (best_time < 0.0005):
        return 'midly_fast'
    return 'slow'

#def evaluate_model(ti):
#    time = ti.xcom_pull(task_ids=None, key='task_id')
 #   if (time < 0.0001 ):
  #      return 'fast'
   # elif (time < 0.0005):
    #    return 'midly_fast'
    #return 'slow'


with DAG("Timeter", start_date=datetime(2021, 1, 1), schedule_interval="@daily", catchup=False) as dag:
    
    primer_metodo = PythonOperator(
        task_id="primer_metodo",
        python_callable=_primer_metodo,
        op_kwargs={'vector': vector}
    )
    
    segundo_metodo = PythonOperator(
        task_id="segundo_metodo",
        python_callable=_segundo_metodo,
        op_kwargs={'vector': vector}
    )

    tercer_metodo = PythonOperator(
        task_id="tercer_metodo",
        python_callable=_tercer_metodo,
        op_kwargs={'vector': vector}
    )

    choose_best_model = BranchPythonOperator(
        task_id="choose_best_model",
        python_callable=choose_best_model
    )
    
    #evaluate_model = BranchPythonOperator(
     #   task_id="evaluate_model",
      #  python_callable=evaluate_model
    #)

    fast = BashOperator(
        task_id="fast",
        bash_command="echo 'fast'"
    )

    midly_fast = BashOperator(
        task_id="midly_fast",
        bash_command="echo 'midly_fast'"
    )

    slow = BashOperator(
        task_id="slow",
        bash_command="echo 'slow'"
    )

#ordenar todo
#primer_metodo >> evaluate_model >> [fast, midly_fast, slow]
#segundo_metodo >> evaluate_model >> [fast, midly_fast, slow]
#tercer_metodo >> evaluate_model >> [fast, midly_fast, slow]

[primer_metodo, segundo_metodo, tercer_metodo] >> choose_best_model >> [fast, midly_fast, slow]