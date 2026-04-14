# Extends the official image so DAGs can run dbt and the repo Python loader.
FROM apache/airflow:2.10.4-python3.12

USER airflow
RUN pip install --no-cache-dir \
    "dbt-core>=1.8.0,<2.0.0" \
    "dbt-postgres>=1.8.0,<2.0.0" \
    "python-dotenv>=1.0.1" \
    "pyyaml>=6.0.1"
