import os
import os
from datetime import timedelta, datetime
import json

from airflow import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.email import send_email
from airflow.utils.decorators import apply_defaults
from airflow.models.baseoperator import BaseOperator
from airflow.operators.bash_operator import BashOperator
from airflow.exceptions import AirflowException
import logging
# custom utils
from utils.job_config import JobConfig
from utils.sql_utils import SqlUtils

JOB_ARGS = JobConfig.get_config()


def send_task_success_email_alert(contextDict, **kwargs):
    print("send_dag_success_email_alert", contextDict)
    dag_id = contextDict['dag'].dag_id
    task_id = contextDict['task_instance'].task_id
    title = "Airflow alert: '{}'.'{}' success".format(dag_id, task_id)
    body = """
        Hi Team, <br>
        <br>
        DAG: '{}'-> Task: '{}' has executed successfully <br>

        <br>
        Airflow bot <br>
        """.format(dag_id, task_id)
    send_email(JOB_ARGS['recipient_email_ids'], title, body)


def send_task_failure_email(contextDict, **kwargs):
    print("send_task_failure_email", contextDict)
    dag_id = contextDict['dag'].dag_id
    task_id = contextDict['task_instance'].task_id
    exception = contextDict['exception']
    title = "Airflow alert: Task '{}' Failed".format(task_id)
    body = """
    Hi Team, <br>
    <br>
    Task: '{}' Got failed in DAG: '{}' <br><br> 
    Below are the details <br><br>
    exception: '{}'
    <br><br>
    Thanks, <br>
     Airflow bot <br>
    """.format(task_id, dag_id, exception)
    print("inside failure_email")
    print(dag_id)
    print(task_id)
    send_email(JOB_ARGS['recipient_email_ids'], title, body)


dag = DAG(
    dag_id="NRD_GDO_Other_Subject_Area",
    default_args=JOB_ARGS['default_args'],
    start_date=datetime(2020, 4, 7),
    schedule_interval=None,  # JOB_ARGS['schedule_interval'],
    catchup=False
)
sql_path = os.path.join('NRD', JOB_ARGS['sql_file_path'])
bq_query = SqlUtils.load_query(sql_path)


def create_dag_task(start_task,ssis_task_name):
    previous_task = ''
    stage_task = []
    for task_id in JOB_ARGS[ssis_task_name]:
        finished = DummyOperator(task_id="{}_completed".format(task_id))
        if previous_task:
            start_task = previous_task
        print(task_id)
        for procs in JOB_ARGS[ssis_task_name][task_id]:
            for i in range(0, len(procs)):
                bq_task = BigQueryOperator(
                    task_id='{}'.format(procs[i][procs[i].rfind('.') + 1:]),
                    sql=bq_query,
                    allow_large_results=True,
                    use_legacy_sql=False,
                    params={"procedure_name": '{}'.format(procs[i])},
                    gcp_conn_id=JOB_ARGS['bq_conn_id'],
                    retries=0,
                    max_retry_delay=timedelta(seconds=1),
                    on_failure_callback=send_task_failure_email,
                    on_success_callback=send_task_success_email_alert,
                    trigger_rule="all_success",
                    dag=dag)
                stage_task.append(bq_task)
            if (len(stage_task) == 1):
                start_task >> stage_task >> finished
            elif (len(stage_task) > 1):
                start_task >> stage_task >> finished
            stage_task.clear()
        previous_task = finished
    return finished

################ - NRD_GDO_Other_Subject_Area - ################
NRD_GDO_Other_Subject_Area_Start = DummyOperator(task_id="NRD_GDO_Other_Subject_Area_Start")
NRD_GDO_Other_Subject_Area_End = DummyOperator(task_id="NRD_GDO_Other_Subject_Area_End")

##Load_Sales_Cloud_Account_Contact_Tables
start_task=NRD_GDO_Other_Subject_Area_Start
ssis_task_name='Load_Sales_Cloud_Account_Contact_Tables'
Load_Sales_Cloud_Account_Contact_Tables_end_task=DummyOperator(task_id="Load_Sales_Cloud_Account_Contact_Tables_end_task")
create_dag_task(start_task,ssis_task_name) >> Load_Sales_Cloud_Account_Contact_Tables_end_task

##Load_Cloud_Account_Contact_Tables
start_task=Load_Sales_Cloud_Account_Contact_Tables_end_task
ssis_task_name='Load_Cloud_Account_Contact_Tables'
Load_Cloud_Account_Contact_Tables_end_task=DummyOperator(task_id="Load_Cloud_Account_Contact_Tables_end_task")
create_dag_task(start_task,ssis_task_name) >> Load_Cloud_Account_Contact_Tables_end_task

##Load_Reporting_Tables
start_task=Load_Cloud_Account_Contact_Tables_end_task
ssis_task_name='Load_Reporting_Tables'
Load_Reporting_Tables_end_task=DummyOperator(task_id="Load_Reporting_Tables_end_task")
create_dag_task(start_task,ssis_task_name) >> Load_Reporting_Tables_end_task

##  
start_task=Load_Reporting_Tables_end_task
ssis_task_name='Load_Sales_Opps_BillFile_Tables'
Load_Sales_Opps_BillFile_Tables_end_task=DummyOperator(task_id="Load_Sales_Opps_BillFile_Tables_end_task")
create_dag_task(start_task,ssis_task_name) >> Load_Sales_Opps_BillFile_Tables_end_task

Load_Sales_Opps_BillFile_Tables_end_task>>NRD_GDO_Other_Subject_Area_End
