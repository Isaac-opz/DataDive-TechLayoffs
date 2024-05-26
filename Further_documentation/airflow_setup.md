## Apache Airflow Setup

To set up Apache Airflow on Linux, follow these steps:

1. Create a virtual environment on your system:
   ```shell
   python -m venv your_environment_name
   ```

2. Activate the virtual environment:
   ```shell
   source your_environment_name/bin/activate
   ```

3. Install Airflow using pip:
   ```shell
   pip install apache-airflow
   ```

4. Install the requirements in your virtual environment from "requirements.txt":
   ```shell
   pip install -r requirements.txt
   ```

5. Define the AIRFLOW_HOME variable as the root of the project (you should be located within it):
   ```shell
   export AIRFLOW_HOME=$(pwd)
   ```

6. Initialize the database:
   ```shell
   airflow db init
   ```

7. Create a user:
   ```shell
   airflow users create --username admin --firstname your_name --lastname your_lastname --role Admin --email your_email
   ```

8. Start the web server:
   ```shell
   airflow webserver --port 8080
   ```

9. Start the scheduler:
   ```shell
   airflow scheduler
   ```

10. Set the DAG path:
    ```shell
    export AIRFLOW__CORE__DAGS_FOLDER=$(pwd)/dags
    ```

11. Access the web server:
    Open your web browser and go to [http://localhost:8080/](http://localhost:8080/)

12. Run the DAG:
    Now you can access the DAGs in the web server and run the one you want.

Please note that these instructions assume you are using a Linux system.