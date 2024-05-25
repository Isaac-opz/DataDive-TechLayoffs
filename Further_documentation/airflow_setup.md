## Apache Airflow Setup
## Linux: Create a virtual environment on your system
```python -m venv your_environment_name```
### Activate the virtual environment
```source your_environment_name/bin/activate```
### Make sure you are located in your virtual environment and install Airflow using pip
``` pip install apache-airflow```
### Install the requirements in your virtual environment from "requirements.txt"
``` pip install -r requirements.txt```
### Define the AIRFLOW_HOME variable as the root of the project (you should be located within it)
``` export AIRFLOW_HOME=$(pwd) ```
### Initialize the database
``` airflow db init ```
### Create a user
``` airflow users create --username admin --firstname your_name --lastname your_lastname --role Admin --email your_email ```
### Start the web server
``` airflow webserver --port 8080 ```
### Start the scheduler
``` airflow scheduler ```
### Dag path
``` export AIRFLOW__CORE__DAGS_FOLDER=$(pwd)/dags ```
### Access the web server
``` http://localhost:8080/ ```
### Run the DAG
Now you can access the DAGs in the web server and run the one you want.