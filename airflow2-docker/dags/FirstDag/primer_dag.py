from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.bash_operator import BashOperator
from
from random import randint
from datetime import datetime

def choose_best_model(ti):
    accuracies = ti.xcom_pull(task_ids=["training_modelA", "training_modelB", "training_modelC"])
    best_accuracy = max(accuracies)
    if (best_accuracy > 7 ):
        return 'accurate'
    elif (best_accuracy > 5):
        return 'midly_accurate'
    return 'inaccurate'

def _training_model():
    return randint(6, 8)

with DAG("primer_dag", start_date=datetime(2021, 1, 1), schedule_interval="@daily", catchup=False) as dag:

    Bienvenida = BashOperator(
        task_id="Bienvenida",
        bash_command="echo 'Hola Chavos, Bienvenidos a Airflow'"
    )

    training_modelA = PythonOperator(
        task_id="training_modelA",
        python_callable=_training_model 
    )

    training_modelB = PythonOperator(
        task_id="training_modelB",
        python_callable=_training_model 
    )

    training_modelC = PythonOperator(
        task_id="training_modelC",
        python_callable=_training_model 
    )

    choose_best_model = BranchPythonOperator(
        task_id="choose_best_model",
        python_callable=choose_best_model
    )

    accurate = BashOperator(
        task_id="accurate",
        bash_command="echo 'accurate'"  
    )

    midly_accurate = BashOperator(
        task_id="midly_accurate",
        bash_command="echo 'midly_accurate'"
    )

    inaccurate = BashOperator(
        task_id="inaccurate",
        bash_command="echo 'inaccurate'"
    )

# Crea una tarea BashOperator para imprimir el resultado de training_modelA
    print_training_modelA = BashOperator(
        task_id="print_training_modelA",
        bash_command="echo 'training_modelA: {{ ti.xcom_pull(task_ids='training_modelA') }}'"
    )

# Crea una tarea BashOperator para imprimir el resultado de training_modelB

    print_training_modelB = BashOperator(
        task_id="print_training_modelB",
        bash_command="echo 'training_modelB: {{ ti.xcom_pull(task_ids='training_modelB') }}'"
    )   

# Crea una tarea BashOperator para imprimir el resultado de training_modelC
    print_training_modelC = BashOperator(
        task_id="print_training_modelC",
        bash_command="echo 'training_modelC: {{ ti.xcom_pull(task_ids='training_modelC') }}'"
    )
#Crea una tarea que mida el tiempo de ejecución de cada modelo
    time_training_modelA = BashOperator(
        task_id="time_training_modelA",
        bash_command="time {{ ti.xcom_pull(task_ids='training_modelA') }}"
    )

 #Acomoda todo en el orden correcto para que el flujo de trabajo funcionen 


    training_modelA >> print_training_modelA
    training_modelB >> print_training_modelB
    training_modelC >> print_training_modelC
    
     
    Bienvenida >> [training_modelA, training_modelB, training_modelC] >> choose_best_model >> [accurate, inaccurate, midly_accurate]

