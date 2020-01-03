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

SPARK_STEPS_1 = [
    {
        'Name': 'calculate_movie_ratings_1',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                '--deploy-mode', 'cluster',
                's3://<s3-bucket>/jobs/movies-analytics.py',
                '-i', 's3://<s3-bucket>/data',
                '-o', 's3://<s3-bucket>/results_1'
            ]
        }
    }
]

SPARK_STEPS_2 = [
    {
        'Name': 'calculate_movie_ratings_2',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                '--deploy-mode', 'cluster',
                's3://<s3-bucket>/jobs/movies-analytics.py',
                '-i', 's3://<s3-bucket>/data',
                '-o', 's3://<s3-bucket>/results_2'
            ]
        }
    }
]

JOB_FLOW_OVERRIDES = {
    "Name": "MoviesAnalytics"
}

with DAG(
        dag_id='emr_job_movies_dag_parallel',
        default_args=DEFAULT_ARGS,
        concurrency=3,
        dagrun_timeout=timedelta(hours=2),
        schedule_interval=None
) as dag:
    cluster_creator = EmrCreateJobFlowOperator(
        task_id='create_emr_cluster',
        job_flow_overrides=JOB_FLOW_OVERRIDES,
        aws_conn_id='aws_default',
        emr_conn_id='emr_default'
    )

    step_adder_1 = EmrAddStepsOperator(
        task_id='movie_analytics_job_1',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        aws_conn_id='aws_default',
        steps=SPARK_STEPS_1
    )

    step_checker_1 = EmrStepSensor(
        task_id='wait_for_analytics_completion_1',
        job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull(task_ids='movie_analytics_job_1', key='return_value')[0] }}",
        aws_conn_id='aws_default'
    )

    step_adder_2 = EmrAddStepsOperator(
        task_id='movie_analytics_job_2',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        aws_conn_id='aws_default',
        steps=SPARK_STEPS_2
    )

    step_checker_2 = EmrStepSensor(
        task_id='wait_for_analytics_completion_2',
        job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull(task_ids='movie_analytics_job_2', key='return_value')[0] }}",
        aws_conn_id='aws_default'
    )

    cluster_remover = EmrTerminateJobFlowOperator(
        task_id='remove_cluster',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        aws_conn_id='aws_default'
    )

    step_adder_1 << cluster_creator
    step_adder_2 << cluster_creator
    step_adder_1 >> step_checker_1 >> cluster_remover
    step_adder_2 >> step_checker_2 >> cluster_remover
