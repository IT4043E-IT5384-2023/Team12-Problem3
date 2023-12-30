from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

default_args = {
  "owner": 'airflow',
  "depends_on_past": False,
  "start_date": datetime(2023, 6, 12),
  "retries": 3,
  "retry_delay": timedelta(minutes=10),
}

with DAG(dag_id="twitter_daily_dag",
         default_args=default_args,
         catchup=False,
         schedule="0 0 * * *"
         ) as dag:
    
    start_task = EmptyOperator(task_id="twitter_daily_dag_start")

    install_package = BashOperator(task_id="install_package",
                                 bash_command="cd ~/Team12-Problem3 && pip install -r requirements.txt",
                                 retries=2,
                                 max_active_tis_per_dag=1)

    tweet_crawler_user = BashOperator(task_id="tweet_crawler_user",
                                 bash_command="cd ~/Team12-Problem3 && python3 jobs/tweet-crawler/tweet_crawler_user.py",
                                 retries=2,
                                 max_active_tis_per_dag=1)

    check_duplicate = BashOperator(task_id="check_duplicate",
                                 bash_command="cd ~/Team12-Problem3 && python3 jobs/data-preprocessing/check_duplicate.py",
                                 retries=2,
                                 max_active_tis_per_dag=1)
    clean_data = BashOperator(task_id="clean_data",
                                 bash_command="cd ~/Team12-Problem3 && python3 jobs/data-preprocessing/clean_data.py",
                                 retries=2,
                                 max_active_tis_per_dag=1)

    preprocessing_data = BashOperator(task_id="preprocessing_data",
                                 bash_command="cd ~/Team12-Problem3 && python3 jobs/data-preprocessing/preprocessing_data.py",
                                 retries=2,
                                 max_active_tis_per_dag=1)
    
    elasticsearch_task = BashOperator(task_id="elasticsearch_task",
                                              bash_command="cd ~/Team12-Problem3 && python3 jobs/write_to_elastic.py",
                                              retries=2,
                                              max_active_tis_per_dag=1)

    model_evaluation = BashOperator(task_id="model_evaluation",
                                 bash_command="cd ~/Team12-Problem3 && python3 jobs/model_evaluation.py",
                                 retries=2,
                                 max_active_tis_per_dag=1)

    end_task = EmptyOperator(task_id="twitter_daily_dag_done")
    
    start_task >> install_package >> tweet_crawler_user >> check_duplicate >> clean_data >> preprocessing_data >> elasticsearch_task >> model_evaluation >> end_task