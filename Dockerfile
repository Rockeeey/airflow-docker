FROM apache/airflow:2.9.0

USER root
# 你需要先做的系统级依赖安装命令（如果有）

# 切换到 airflow 用户，避免权限问题
USER airflow

RUN pip install dbt-core==1.10.1 dbt-snowflake==1.9.4 "pyarrow<19.0.0"
