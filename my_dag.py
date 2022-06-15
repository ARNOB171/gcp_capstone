"""Example Airflow DAG that creates a Cloud Dataproc cluster, runs the Hadoop
wordcount example, and deletes the cluster.
This DAG relies on three Airflow variables
https://airflow.apache.org/concepts.html#variables
* gcp_project - Google Cloud Project to use for the Cloud Dataproc cluster.
* gce_zone - Google Compute Engine zone where Cloud Dataproc cluster should be
  created.
* gcs_bucket - Google Cloud Storage bucket to use for result of Hadoop job.
  See https://cloud.google.com/storage/docs/creating-buckets for creating a
  bucket.
"""
import datetime
import os
from airflow import models
from airflow.utils import trigger_rule
from airflow.operators import dummy_operator
from datetime import timedelta
from airflow.operators.bash_operator import BashOperator
# Output file for Cloud Dataproc job.
# Path to Hadoop wordcount example available on every Dataproc cluster.

yesterday = datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(1),
    datetime.datetime.min.time())
default_dag_args = {
    # Setting start date as yesterday starts the DAG immediately when it is
    # detected in the Cloud Storage bucket.
    'start_date': yesterday,
    # To email on failure or retry set 'email' arg to your email and enable
    # emailing here.
    'email_on_failure': False,
    'email_on_retry': False,
    # If a task fails, retry it once after waiting at least 5 minutes
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
    'project_id': models.Variable.get('gcp_project')
}
# [START composer_hadoop_schedule]
with models.DAG(
    'cloud_composer_capstone',
    schedule_interval=None,
    default_args=default_dag_args) as dag:

  start = dummy_operator.DummyOperator(
        task_id='start',
        trigger_rule='all_success'
    )
  end = dummy_operator.DummyOperator(
        task_id='end',
        trigger_rule='all_success'
    )
  do_stuff1 = BashOperator(
        task_id="task_1",
        bash_command="python /home/airflow/gcs/data/scripts/etl_capstone.py",
    )
  start >> do_stuff1 >> end
    # [END composer_hadoop_steps]
