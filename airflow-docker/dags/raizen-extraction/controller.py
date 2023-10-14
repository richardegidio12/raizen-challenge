from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy import DummyOperator

import pandas as pd 
from subprocess import  Popen, PIPE
import pendulum
from datetime import datetime
import unidecode
import logging
#from raizenextraction.utils_airflow import *


DAG_NAME = "raizen_extraction_anp"

dag = DAG(
    DAG_NAME,
    schedule= None,
    default_args={
        'owner':'Data Engineering',
        'depends_on_past': False,
        'retries': 2 },
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
)



def prepare_dataframe(layer:str,filename:str, worksheet:str, product:str=None):
    if layer == 'staging':
        df = pd.read_excel(f"/opt/airflow/dags/raizen-extraction/{layer}/{filename}.xls", sheet_name=worksheet)
        df.columns = [unidecode.unidecode(col).lower() for col in df.columns]
        df.columns = ['combustivel', 'ano', 'regiao','estado', '01','02','03','04','05','06','07','08','09','10','11','12','total']
        months = df.columns[4:-1]
        df = df.melt(id_vars=['combustivel', 'ano', 'regiao','estado'], value_vars=months, var_name='mes', value_name='volume')
        return df.round({'volume':1})
    elif layer == 'trusted':
        df = pd.read_csv(f"/opt/airflow/dags/raizen-extraction/{layer}/{product}-{filename}.csv")
        df['year_month'] = pd.to_datetime(df['year_month'])
        return df.round({'volume':1})


def count_validation(layer:str, filename:str, product:str, worksheet:int=None):
    if layer == 'trusted':
        pv_trusted = prepare_dataframe(layer=layer, filename=filename, worksheet=worksheet, product=product)
        pv_trusted['ano'] = pv_trusted['year_month'].dt.year
        pv_trusted = pv_trusted.groupby('ano').sum('volume')
        return pv_trusted.to_dict()
    elif layer =='staging':
        pv_raw = prepare_dataframe(layer=layer, filename=filename, worksheet=worksheet, product=product)
        pv_raw = pd.pivot_table(pv_raw,values = 'volume', index=['ano'], aggfunc="sum").groupby('ano').sum('volume')
        return pv_raw.to_dict()


def is_totals_equal(dict1:dict, dict2:dict):

    is_dict_equal = all(dict1[i] ==  dict2[i] for i in dict1)
    if is_dict_equal:
        return True
    else:
        return False


def libre_office_file_generator():
    p = Popen(['libreoffice', '--headless', '--convert-to', 'xls', '--outdir',
            '/opt/airflow/dags/raizen-extraction/staging', '/opt/airflow/dags/raizen-extraction/raw/vendas-combustiveis-m3.xls'], stdout=PIPE, stderr=PIPE, text=True)
    output, errors = p.communicate()
    if p.returncode != 0: 
        raise("Process of libreOffice file Generator failed %d %s" % (p.returncode, output))
    print(output)
    print(errors)
    
    
def staging_to_trusted(filename:str , worksheet:int, product:str):
    df = pd.read_excel(f"/opt/airflow/dags/raizen-extraction/staging/{filename}.xls", sheet_name=worksheet)
    df.columns = [unidecode.unidecode(col).lower() for col in df.columns]
    df.columns = ['combustivel', 'ano', 'regiao','estado', '01','02','03','04','05','06','07','08','09','10','11','12','total']
    months = df.columns[4:-1]
    df = df.melt(id_vars=['combustivel', 'ano', 'regiao','estado'], value_vars=months, var_name='mes', value_name='volume')
    df['combustivel'] =  df['combustivel'].str.replace("(m3)", "")
    df['unit'] = "m3"
    df['year_month'] = pd.to_datetime( df['ano'].astype(str) + '-' + df['mes'], format="%Y-%m" )
    df.drop(columns=['regiao', 'ano', 'mes'], inplace=True)
    df.rename(columns={"combustivel":"product","estado": "uf"}, inplace=True)
    df["created_at"] = pd.Timestamp.today()
    df.fillna(0, inplace=True)
    df.to_csv(f'/opt/airflow/dags/raizen-extraction/trusted/{product}-{filename}.csv', date_format='%Y-%m-%d',index=False)


def count_validation_stage(product:str):
    if product == 'fuels_derivated':
        worksheet = 1
    elif product == 'diesel':
        worksheet = 2
    trusted = count_validation(layer='trusted', filename='vendas-combustiveis-m3', product=product)
    staging = count_validation(layer='staging', worksheet=worksheet, filename='vendas-combustiveis-m3', product=product)
    if is_totals_equal(staging , trusted):
        logging.info("Process Validated!")
    else:
        raise('ERROR : Check the files to confirm!')



def fuels_refined_writing(product:str, filename:str):
    df = pd.read_csv(f"/opt/airflow/dags/raizen-extraction/trusted/{product}-{filename}.csv")
    df.to_parquet(f"/opt/airflow/dags/raizen-extraction/refined/{product}-{filename}",compression='snappy', partition_cols=['created_at'])
    logging.info(f"Process Fineshed: File created at path: /opt/airflow/dags/raizen-extraction/refined/{product}-{filename}")


start_task = DummyOperator(task_id='start_task', dag=dag)


libre_office_file  = PythonOperator(
        task_id='libre_office_file', 
        python_callable=libre_office_file_generator,
        dag=dag)

fuels_staging_to_trusted  = PythonOperator(
        task_id='fuels_staging_to_trusted', 
        python_callable=staging_to_trusted,
        op_kwargs={'filename': 'vendas-combustiveis-m3', 
                   'worksheet': 1,
                   'product': 'fuels_derivated'},
        dag=dag)

diesel_staging_to_trusted  = PythonOperator(
        task_id='diesel_staging_to_trusted', 
        python_callable=staging_to_trusted,
        op_kwargs={'filename': 'vendas-combustiveis-m3', 
                   'worksheet': 2,
                   'product': 'diesel'},
        dag=dag)

diesel_count_validation  = PythonOperator(
        task_id='diesel_count_validation', 
        python_callable=count_validation_stage,
        op_kwargs={'product': 'diesel'},
        dag=dag)

fuels_count_validation  = PythonOperator(
        task_id='fuels_count_validation', 
        python_callable=count_validation_stage,
        op_kwargs={'product': 'fuels_derivated'},
        dag=dag)


fuels_trusted_to_refined  = PythonOperator(
        task_id='fuels_trusted_to_refined', 
        python_callable=fuels_refined_writing,
        op_kwargs={'product': 'fuels_derivated',
                   'filename': 'vendas-combustiveis-m3'},
        dag=dag)


diesel_trusted_to_refined  = PythonOperator(
        task_id='diesel_trusted_to_refined', 
        python_callable=fuels_refined_writing,
        op_kwargs={'product': 'diesel',
                   'filename': 'vendas-combustiveis-m3'},
        dag=dag)

end_task = DummyOperator(task_id='end_task', trigger_rule='one_success', dag=dag)

    
start_task >> libre_office_file >> [fuels_staging_to_trusted, diesel_staging_to_trusted] 
fuels_staging_to_trusted >> fuels_count_validation >> fuels_trusted_to_refined
diesel_staging_to_trusted >> diesel_count_validation >> diesel_trusted_to_refined
[fuels_trusted_to_refined, diesel_trusted_to_refined] >> end_task