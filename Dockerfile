FROM apache/airflow:2.9.0

USER root

USER airflow

RUN pip install dbt-core==1.10.1 dbt-snowflake==1.9.4 "pyarrow<19.0.0"
