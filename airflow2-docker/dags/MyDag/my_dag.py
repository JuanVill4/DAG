from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd

# ---------------------------------CONFIG_FILE----------------------------------------
CONFIG = {"ejemplo_config_file": {
    "datos": [['Airflow', 'PythonOperator', "llamadas de python", ''],
              ['Airflow', 'BranchPythonOperator', 'seguir una ruta', ''],
              ['Airflow', 'BashOperator', 'comandos en un shell', '']],
    "columnas": ['herramienta', 'operador', 'funcion', 'fecha'],
    "filas": ['F1', 'F2', 'F3']
},
    "datos": [
        {'Nombre': 'OMAR MENDOZA', 'Departamento': 'TECNOLOGÍA', 'Aprendizaje': 'DESARROLLO DE DAGS'},
        {'Nombre': 'JUANJO VILLANUEVA',  'Departamento': 'TECNOLOGÍA', 'Aprendizaje': 'DESARROLLO DE DAGS'},
        {'Nombre': 'VICTOR VAZQUEZ', 'Departamento': 'TECNOLOGÍA', 'Aprendizaje': 'DESARROLLO DE DAGS'},
        {'Nombre': 'SEBASTIAN MARTINEZ', 'Departamento': 'TECNOLOGÍA', 'Aprendizaje': 'DESARROLLO DE DAGS'}
    ]
}
# -----------------------------------------------------------------------------------

# ---------------------------------OPERATOR----------------------------------------
FULL_DATE_FORMAT = "%Y-%m-%d"


def crear_tabla(datos, columnas, filas):
    df = pd.DataFrame(datos, columns=columnas, index=filas)
    df['fecha'] = datetime.now().strftime(FULL_DATE_FORMAT)
    pd.options.display.max_rows = None
    pd.options.display.max_columns = None
    print(f"PRIMERA TABLA {df}")


def crear_tabla2(datos):
    df = pd.DataFrame(datos)
    pd.options.display.max_rows = None
    pd.options.display.max_columns = None
    print(f"veamos su contenido {df}")


# ---------------------------------------------------------------------------------

with DAG("CREANDO_TABLAS_CON_PANDAS",
         start_date=datetime(2023, 3, 9),
         schedule_interval='@monthly',
         catchup=False) as dag:
    start_task = EmptyOperator(
        task_id="start_task",
    )

    crear_tabla_1_task = PythonOperator(
        task_id="crear_tabla_1_task",
        python_callable=crear_tabla,
        op_kwargs={
            "datos": CONFIG['ejemplo_config_file']['datos'],
            "columnas": CONFIG['ejemplo_config_file']['columnas'],
            "filas": CONFIG['ejemplo_config_file']['filas']
        }
    )
    crear_tabla_2_task = PythonOperator(
        task_id="crear_tabla_2_task",
        python_callable=crear_tabla2,
        op_kwargs={
            "datos": CONFIG['datos']
        }
    )

    end_task = EmptyOperator(
        task_id="end_task",
    )

    start_task >> crear_tabla_1_task >> crear_tabla_2_task >> end_task

