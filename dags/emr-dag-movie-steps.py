from datetime import timedelta

import airflow
from airflow import DAG
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.contrib.operators.emr_terminate_job_flow_operator import EmrTerminateJobFlowOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor

DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(2),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False
}

SPARK_TEST_STEPS = [
    {
        'Name': 'calculate_movie_ratings',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                '--deploy-mode', 'cluster',
                's3://<s3-bucket>/jobs/avg_rating.py',
                '-i', 's3://<s3-bucket>/data',
                '-o', 's3://<s3-bucket>/results'
            ]
        }
    }
]

JOB_FLOW_OVERRIDES = {
    "Name": "MoviesAnalytics"
}

with DAG(
        dag_id='emr_job_movies_dag',
        default_args=DEFAULT_ARGS,
        dagrun_timeout=timedelta(hours=2),
        schedule_interval=None
) as dag:
    create_cluster = EmrCreateJobFlowOperator(
        task_id='create_emr_cluster',
        job_flow_overrides=JOB_FLOW_OVERRIDES,
        aws_conn_id='aws_default',
        emr_conn_id='emr_default'
    )

    add_step = EmrAddStepsOperator(
        task_id='movie_analytics_job',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        aws_conn_id='aws_default',
        steps=SPARK_TEST_STEPS
    )

    wait_for_step = EmrStepSensor(
        task_id='wait_for_analytics_completion',
        job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull(task_ids='movie_analytics_job', key='return_value')[0] }}",
        aws_conn_id='aws_default'
    )

    terminate_cluster = EmrTerminateJobFlowOperator(
        task_id='remove_cluster',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        aws_conn_id='aws_default',
        trigger_rule='all_done'
    )

    create_cluster >> add_step >> wait_for_step >> terminate_cluster
