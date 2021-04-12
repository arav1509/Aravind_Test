import os
import os
from datetime import timedelta, datetime
import json

from airflow import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.email import send_email
import pprint as pp
from airflow.operators.dagrun_operator import TriggerDagRunOperator
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
    dag_id="NRD_GDO_Stage_Tables",
    default_args=JOB_ARGS['default_args'],
    start_date=datetime(2020, 4, 7),
    schedule_interval=None,  # JOB_ARGS['schedule_interval'],
    catchup=False
)
sql_path = os.path.join('NRD', JOB_ARGS['sql_file_path'])
bq_query = SqlUtils.load_query(sql_path)

################ - NRD_Stage_Dedicated_Invocie - ################
NRD_Stage_Dedicated_Invocie_Start = DummyOperator(task_id="NRD_Stage_Dedicated_Invocie_Start")
NRD_Stage_Dedicated_Invocie_End = DummyOperator(task_id="NRD_Stage_Dedicated_Invocie_End")

previous_task = ''
stage_task = []
for task_id in JOB_ARGS['NRD_Stage_Dedicated_Invocie']:
    finished = DummyOperator(task_id="{}_completed".format(task_id))
    if previous_task:
        NRD_Stage_Dedicated_Invocie_Start = previous_task
    print(task_id)
    for procs in JOB_ARGS['NRD_Stage_Dedicated_Invocie'][task_id]:
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
            NRD_Stage_Dedicated_Invocie_Start >> stage_task >> finished
        elif (len(stage_task) > 1):
            NRD_Stage_Dedicated_Invocie_Start >> stage_task >> finished
        stage_task.clear()
    previous_task = finished

finished >> NRD_Stage_Dedicated_Invocie_End


################ - NRD_Stage_Dedicated_Invoice_Audit - ################
NRD_Stage_Dedicated_Invoice_Audit_Start = DummyOperator(task_id="NRD_Stage_Dedicated_Invoice_Audit_Start")
NRD_Stage_Dedicated_Invoice_Audit_End = DummyOperator(task_id="NRD_Stage_Dedicated_Invoice_Audit_End")
NRD_Stage_Dedicated_Invocie_End >> NRD_Stage_Dedicated_Invoice_Audit_Start
previous_task = ''
stage_task = []
for task_id in JOB_ARGS['NRD_Stage_Dedicated_Invoice_Audit']:
    finished = DummyOperator(task_id="{}_completed".format(task_id))
    if previous_task:
        NRD_Stage_Dedicated_Invoice_Audit_Start = previous_task
    print(task_id)
    for procs in JOB_ARGS['NRD_Stage_Dedicated_Invoice_Audit'][task_id]:
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
            NRD_Stage_Dedicated_Invoice_Audit_Start >> stage_task >> finished
        elif (len(stage_task) > 1):
            NRD_Stage_Dedicated_Invoice_Audit_Start >> stage_task >> finished
        stage_task.clear()
    previous_task = finished


finished >> NRD_Stage_Dedicated_Invoice_Audit_End



################ - NRD_Stage_Update_Invoice_Date - ################
NRD_Stage_Update_Invoice_Date_Start = DummyOperator(task_id="NRD_Stage_Update_Invoice_Date_Start")
NRD_Stage_Update_Invoice_Date_End = DummyOperator(task_id="NRD_Stage_Update_Invoice_Date_End")
NRD_Stage_Dedicated_Invoice_Audit_End >> NRD_Stage_Update_Invoice_Date_Start
previous_task = ''
stage_task = []
for task_id in JOB_ARGS['NRD_Stage_Update_Invoice_Date']:
    finished = DummyOperator(task_id="{}_completed".format(task_id))
    if previous_task:
        NRD_Stage_Update_Invoice_Date_Start = previous_task
    print(task_id)
    for procs in JOB_ARGS['NRD_Stage_Update_Invoice_Date'][task_id]:
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
            NRD_Stage_Update_Invoice_Date_Start >> stage_task >> finished
        elif (len(stage_task) > 1):
            NRD_Stage_Update_Invoice_Date_Start >> stage_task >> finished
        stage_task.clear()
    previous_task = finished


finished >> NRD_Stage_Update_Invoice_Date_End



################ - NRD_Stage_Cloud_Invoice - ################
NRD_Stage_Cloud_Invoice_Start = DummyOperator(task_id="NRD_Stage_Cloud_Invoice_Start")
NRD_Stage_Cloud_Invoice_End = DummyOperator(task_id="NRD_Stage_Cloud_Invoice_End")
previous_task = ''
stage_task = []
for task_id in JOB_ARGS['NRD_Stage_Cloud_Invoice']:
    finished = DummyOperator(task_id="{}_completed".format(task_id))
    if previous_task:
        NRD_Stage_Cloud_Invoice_Start = previous_task
    print(task_id)
    for procs in JOB_ARGS['NRD_Stage_Cloud_Invoice'][task_id]:
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
            NRD_Stage_Cloud_Invoice_Start >> stage_task >> finished
        elif (len(stage_task) > 1):
            NRD_Stage_Cloud_Invoice_Start >> stage_task >> finished
        stage_task.clear()
    previous_task = finished


finished >> NRD_Stage_Cloud_Invoice_End



################ - NRD_Stage_Cloud_Invoice_Audit - ################
NRD_Stage_Cloud_Invoice_Audit_Start = DummyOperator(task_id="NRD_Stage_Cloud_Invoice_Audit_Start")
NRD_Stage_Cloud_Invoice_Audit_End = DummyOperator(task_id="NRD_Stage_Cloud_Invoice_Audit_End")
NRD_Stage_Cloud_Invoice_End >> NRD_Stage_Cloud_Invoice_Audit_Start
previous_task = ''
stage_task = []
for task_id in JOB_ARGS['NRD_Stage_Cloud_Invoice_Audit']:
    finished = DummyOperator(task_id="{}_completed".format(task_id))
    if previous_task:
        NRD_Stage_Cloud_Invoice_Audit_Start = previous_task
    print(task_id)
    for procs in JOB_ARGS['NRD_Stage_Cloud_Invoice_Audit'][task_id]:
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
            NRD_Stage_Cloud_Invoice_Audit_Start >> stage_task >> finished
        elif (len(stage_task) > 1):
            NRD_Stage_Cloud_Invoice_Audit_Start >> stage_task >> finished
        stage_task.clear()
    previous_task = finished


finished >> NRD_Stage_Cloud_Invoice_Audit_End



################ - NRD_Stage_Email_Apps_Invoice - ################
NRD_Stage_Email_Apps_Invoice_Start = DummyOperator(task_id="NRD_Stage_Email_Apps_Invoice_Start")
NRD_Stage_Email_Apps_Invoice_End = DummyOperator(task_id="NRD_Stage_Email_Apps_Invoice_End")
previous_task = ''
stage_task = []
for task_id in JOB_ARGS['NRD_Stage_Email_Apps_Invoice']:
    finished = DummyOperator(task_id="{}_completed".format(task_id))
    if previous_task:
        NRD_Stage_Email_Apps_Invoice_Start = previous_task
    print(task_id)
    for procs in JOB_ARGS['NRD_Stage_Email_Apps_Invoice'][task_id]:
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
            NRD_Stage_Email_Apps_Invoice_Start >> stage_task >> finished
        elif (len(stage_task) > 1):
            NRD_Stage_Email_Apps_Invoice_Start >> stage_task >> finished
        stage_task.clear()
    previous_task = finished


finished >> NRD_Stage_Email_Apps_Invoice_End



################ - NRD_Stage_Xref_Tables - ################
NRD_Stage_Xref_Tables_Start = DummyOperator(task_id="NRD_Stage_Xref_Tables_Start")
NRD_Stage_Xref_Tables_End = DummyOperator(task_id="NRD_Stage_Xref_Tables_End")
NRD_Stage_Update_Invoice_Date_End >>  NRD_Stage_Xref_Tables_Start
NRD_Stage_Cloud_Invoice_Audit_End >> NRD_Stage_Xref_Tables_Start
NRD_Stage_Email_Apps_Invoice_End >> NRD_Stage_Xref_Tables_Start
previous_task = ''
stage_task = []
for task_id in JOB_ARGS['NRD_Stage_Xref_Tables']:
    finished = DummyOperator(task_id="{}_completed".format(task_id))
    if previous_task:
        NRD_Stage_Xref_Tables_Start = previous_task
    print(task_id)
    for procs in JOB_ARGS['NRD_Stage_Xref_Tables'][task_id]:
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
            NRD_Stage_Xref_Tables_Start >> stage_task >> finished
        elif (len(stage_task) > 1):
            NRD_Stage_Xref_Tables_Start >> stage_task >> finished
        stage_task.clear()
    previous_task = finished


finished >> NRD_Stage_Xref_Tables_End



################ - NRD_Stage_Cloud_Hosting_Products - ################
NRD_Stage_Cloud_Hosting_Products_Start = DummyOperator(task_id="NRD_Stage_Cloud_Hosting_Products_Start")
NRD_Stage_Cloud_Hosting_Products_End = DummyOperator(task_id="NRD_Stage_Cloud_Hosting_Products_End")
NRD_Stage_Xref_Tables_End>>NRD_Stage_Cloud_Hosting_Products_Start
previous_task = ''
stage_task = []
for task_id in JOB_ARGS['NRD_Stage_Cloud_Hosting_Products']:
    finished = DummyOperator(task_id="{}_completed".format(task_id))
    if previous_task:
        NRD_Stage_Cloud_Hosting_Products_Start = previous_task
    print(task_id)
    for procs in JOB_ARGS['NRD_Stage_Cloud_Hosting_Products'][task_id]:
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
            NRD_Stage_Cloud_Hosting_Products_Start >> stage_task >> finished
        elif (len(stage_task) > 1):
            NRD_Stage_Cloud_Hosting_Products_Start >> stage_task >> finished
        stage_task.clear()
    previous_task = finished


finished >> NRD_Stage_Cloud_Hosting_Products_End


################ - NRD_Stage_Update_Is_Transaction - ################
NRD_Stage_Update_Is_Transaction_Start = DummyOperator(task_id="NRD_Stage_Update_Is_Transaction_Start")
NRD_Stage_Update_Is_Transaction_End = DummyOperator(task_id="NRD_Stage_Update_Is_Transaction_End")
NRD_Stage_Update_Invoice_Date_End >>  NRD_Stage_Update_Is_Transaction_Start
NRD_Stage_Cloud_Invoice_Audit_End >> NRD_Stage_Update_Is_Transaction_Start
NRD_Stage_Email_Apps_Invoice_End >> NRD_Stage_Update_Is_Transaction_Start
previous_task = ''
stage_task = []
for task_id in JOB_ARGS['NRD_Stage_Update_Is_Transaction']:
    finished = DummyOperator(task_id="{}_completed".format(task_id))
    if previous_task:
        NRD_Stage_Update_Is_Transaction_Start = previous_task
    print(task_id)
    for procs in JOB_ARGS['NRD_Stage_Update_Is_Transaction'][task_id]:
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
            NRD_Stage_Update_Is_Transaction_Start >> stage_task >> finished
        elif (len(stage_task) > 1):
            NRD_Stage_Update_Is_Transaction_Start >> stage_task >> finished
        stage_task.clear()
    previous_task = finished


finished >> NRD_Stage_Update_Is_Transaction_End


################ - NRD_Stage_Credit_BRM - ################
NRD_Stage_Credit_BRM_Start = DummyOperator(task_id="NRD_Stage_Credit_BRM_Start")
NRD_Stage_Credit_BRM_End = DummyOperator(task_id="NRD_Stage_Credit_BRM_End")
NRD_Stage_Update_Is_Transaction_End >> NRD_Stage_Credit_BRM_Start
previous_task = ''
stage_task = []
for task_id in JOB_ARGS['NRD_Stage_Credit_BRM']:
    finished = DummyOperator(task_id="{}_completed".format(task_id))
    if previous_task:
        NRD_Stage_Credit_BRM_Start = previous_task
    print(task_id)
    for procs in JOB_ARGS['NRD_Stage_Credit_BRM'][task_id]:
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
            NRD_Stage_Credit_BRM_Start >> stage_task >> finished
        elif (len(stage_task) > 1):
            NRD_Stage_Credit_BRM_Start >> stage_task >> finished
        stage_task.clear()
    previous_task = finished


finished >> NRD_Stage_Credit_BRM_End



################ - NRD_Stage_Dim_Invoice_And_Attributes - ################
NRD_Stage_Dim_Invoice_And_Attributes_Start = DummyOperator(task_id="NRD_Stage_Dim_Invoice_And_Attributes_Start")
NRD_Stage_Dim_Invoice_And_Attributes_End = DummyOperator(task_id="NRD_Stage_Dim_Invoice_And_Attributes_End")

NRD_Stage_Credit_BRM_End >> NRD_Stage_Dim_Invoice_And_Attributes_Start
NRD_Stage_Cloud_Hosting_Products_End >> NRD_Stage_Dim_Invoice_And_Attributes_Start

previous_task = ''
stage_task = []
for task_id in JOB_ARGS['NRD_Stage_Dim_Invoice_And_Attributes']:
    finished = DummyOperator(task_id="{}_completed".format(task_id))
    if previous_task:
        NRD_Stage_Dim_Invoice_And_Attributes_Start = previous_task
    print(task_id)
    for procs in JOB_ARGS['NRD_Stage_Dim_Invoice_And_Attributes'][task_id]:
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
            NRD_Stage_Dim_Invoice_And_Attributes_Start >> stage_task >> finished
        elif (len(stage_task) > 1):
            NRD_Stage_Dim_Invoice_And_Attributes_Start >> stage_task >> finished
        stage_task.clear()
    previous_task = finished


finished >> NRD_Stage_Dim_Invoice_And_Attributes_End

def conditionally_trigger(context, dag_run_obj):
    if context['params']['condition_param']:
        dag_run_obj.payload = {
                'message': context['params']['message']
            }
        pp.pprint(dag_run_obj.payload)
        return dag_run_obj

trigger_NRD_GDO_DIM_Tables_dag = TriggerDagRunOperator(
        task_id="trigger_NRD_GDO_DIM_Tables_dag",
        trigger_dag_id="NRD_GDO_DIM_Tables",
        provide_context=True,
        python_callable=conditionally_trigger,
        trigger_rule="all_success",
        params={
            'condition_param': True,
            'message': 'NRD_GDO_DIM_Tables Executed'
        },
    )

NRD_Stage_Dim_Invoice_And_Attributes_End >> trigger_NRD_GDO_DIM_Tables_dag