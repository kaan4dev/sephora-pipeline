# sephora-pipeline

## Airflow orchestration

The repository now includes an Airflow DAG (`dags/sephora_etl_dag.py`) that runs the full extract → transform → load pipeline by reusing the existing scripts in `src/`. Each task executes the corresponding script with the project root on `PYTHONPATH`, so the same code paths that you run manually are orchestrated in Airflow.

### Running the DAG with Docker Compose

1. Set `AIRFLOW_UID=$(id -u)` in your shell (Airflow uses it for file permissions inside containers).
2. Copy the official Airflow `docker-compose.yaml` template (see <https://airflow.apache.org/docs/apache-airflow/stable/docker-compose.yaml>) into the repository root and keep the default services (webserver, scheduler, triggerer, postgres, redis).
3. Mount this project into the webserver/scheduler containers, for example:
   ```yaml
   volumes:
     - ./:/opt/airflow/sephora-pipeline
   environment:
     - PYTHONPATH=/opt/airflow/sephora-pipeline
   ```
   Then update the `AIRFLOW__CORE__DAGS_FOLDER` environment variable to `/opt/airflow/sephora-pipeline/dags`.
4. Add the repository requirements to the Airflow image by extending the `Dockerfile` (e.g. install the packages in `requirements.txt`).
5. Run `docker compose up airflow-init` once, followed by `docker compose up` to start the stack. The DAG named `sephora_etl` will appear in the Airflow UI; trigger it manually or let it run on the daily schedule.

Azure credentials (`AZURE_STORAGE_ACCOUNT_NAME`, `AZURE_STORAGE_ACCOUNT_KEY`, `AZURE_FILESYSTEM_NAME`) must be available in the Airflow environment (environment variables or connections) for the load step to succeed.
