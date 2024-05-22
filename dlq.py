import logging
import os
from pyspark.sql import SparkSession
from pymongo import MongoClient
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from datetime import datetime
import streamlit as st
import atexit

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', filename='DAS.log')

# Load configurations
MONGO_CONNECTION_STRING = os.getenv("MONGO_CONNECTION_STRING",\
                                    "mongodb+srv://Ramees:Ramees@cluster1.vcarrff.mongodb.net/")
SPARK_MASTER = os.getenv("SPARK_MASTER", "spark://0.0.0.0:7077")

# MongoDB connection function
@st.cache_resource
def connect_to_mongodb(connection_string):
    try:
        client = MongoClient(connection_string)
        database = client["DAS"]
        task_collection = database["Task"]
        dlq_collection = database["dead_letter_queue"]  # DLQ Collection
        logging.info("Connected to MongoDB Atlas")
        return client, database, task_collection, dlq_collection
    except Exception as e:
        logging.error(f"Failed to connect to MongoDB Atlas: {e}")
        raise SystemExit("Failed to connect to MongoDB Atlas. Exiting script.")

@st.cache_data
def fetch_tasks():
    try:
        _, _, task_collection, _ = connect_to_mongodb(MONGO_CONNECTION_STRING)
        tasks = list(task_collection.find({}))
        if tasks:
            logging.info("Tasks retrieved from the collection")
        else:
            logging.info("No tasks found in the collection")
        return tasks
    except Exception as e:
        logging.error(f"Failed to fetch tasks from the collection: {e}")
        return []

# Function to run a Spark job
def run_spark_job(spark_master, recipe_id, script_path):
    try:
        logging.info(f"Initializing Spark session with master: {spark_master}")
        spark = SparkSession.builder.master(spark_master).appName("SparkJob").getOrCreate()

        logging.info(f"Running Spark job for recipe_id: {recipe_id} using script: {script_path}")
        with open(script_path, 'r') as file:
            script_code = file.read()
        exec(script_code)
        logging.info("Spark job execution successful")
        spark.stop()
    except Exception as e:
        logging.error(f"Error running Spark job: {e}")
        move_to_dlq(recipe_id, script_path, str(e))
        raise

def move_to_dlq(recipe_id, script_path, error_message):
    try:
        _, _, _, dlq_collection = connect_to_mongodb(MONGO_CONNECTION_STRING)
        dlq_task = {
            "recipe_id": recipe_id,
            "code_path": script_path,
            "error_message": error_message,
            "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        dlq_collection.insert_one(dlq_task)
        logging.info(f"Task {recipe_id} moved to DLQ")
    except Exception as e:
        logging.error(f"Failed to move task {recipe_id} to DLQ: {e}")

def fetch_dlq_tasks():
    try:
        _, _, _, dlq_collection = connect_to_mongodb(MONGO_CONNECTION_STRING)
        tasks = list(dlq_collection.find({}))
        if tasks:
            logging.info("DLQ tasks retrieved from the collection")
        else:
            logging.info("No tasks found in the DLQ collection")
        return tasks
    except Exception as e:
        logging.error(f"Failed to fetch tasks from DLQ collection: {e}")
        return []
def retry_dlq_task(task, num_retries):
    try:
        for attempt in range(num_retries):
            try:
                run_spark_job(SPARK_MASTER, task['recipe_id'], task['code_path'])
                _, _, _, dlq_collection = connect_to_mongodb(MONGO_CONNECTION_STRING)
                dlq_collection.delete_one({"_id": task["_id"]})
                logging.info(f"Task {task['recipe_id']} retried and removed from DLQ")
                st.success(f"Task '{task['recipe_id']}' retried successfully!")
                return  # Exit the function after successful retry
            except Exception as e:
                logging.error(f"Attempt {attempt+1} to retry task {task['recipe_id']} failed: {e}")
                if attempt == num_retries - 1:
                    st.error(f"Failed to retry task '{task['recipe_id']}' after {num_retries} attempts.")
                    return
    except Exception as e:
        logging.error(f"Failed to retry task {task['recipe_id']}: {e}")
        st.error(f"Failed to retry task '{task['recipe_id']}' due to an unexpected error.")

def on_server_shutdown():
    logging.info("Streamlit server is shutting down")
    logging.info('SERVER IS DOWN')

def notify_status():
    logging.info("Server is running and monitoring tasks")

def main():
    st.markdown(
        "<h1 style='text-align:center; background-color:blueviolet; color:yellow;border-radius: 20px;'>DAS USER INTERFACE</h1>",
        unsafe_allow_html=True)

    try:
        client, database, task_collection, dlq_collection = connect_to_mongodb(MONGO_CONNECTION_STRING)
    except SystemExit as e:
        st.error(str(e))
        return

    with st.expander("Important Notice:"):
        st.warning("Please save your work regularly. The server may shut down without warning.")
        st.info("In case of a server shutdown, you may lose unsaved work. We apologize for any inconvenience.")

    scheduler = BackgroundScheduler()
    scheduler.add_job(notify_status, 'interval', minutes=1)
    scheduler.start()

    st.sidebar.title("TASK MANAGEMENT")

    st.sidebar.header("Add New Task")
    recipe_id = st.sidebar.text_input("Recipe ID")
    script_path = st.sidebar.text_input("Script Path")
    start_time = st.sidebar.time_input("Start Time")
    end_time = st.sidebar.time_input("End Time")
    cron_expression = st.sidebar.text_input("Cron Expression (Optional, e.g., '*/1 * * * *')")

    if st.sidebar.button("Schedule Task"):
        try:
            start_time_str = start_time.strftime('%H:%M:%S')
            end_time_str = end_time.strftime('%H:%M:%S')
            if not cron_expression:
                cron_expression = "*/2 * * * *"

            task = {
                "recipe_id": recipe_id,
                "cron_expression": cron_expression,
                "code_path": script_path,
                "start_time": start_time_str,
                "end_time": end_time_str
            }
            task_collection.insert_one(task)
            st.sidebar.success("Task scheduled successfully!")
        except Exception as e:
            st.sidebar.error(f"Error scheduling task: {e}")

    st.markdown("<h2 style='text-align:center;color: #339966;'>SELECT TASK TO RUN</h2>", unsafe_allow_html=True)
    tasks = fetch_tasks()

    if not tasks:
        st.info("No tasks found in the collection.")
    else:
        for task in tasks:
            task["start_time"] = datetime.strptime(task["start_time"], '%H:%M:%S').time()
            task["end_time"] = datetime.strptime(task["end_time"], '%H:%M:%S').time()

        task_names = [task["recipe_id"] for task in tasks]
        selected_task = st.selectbox("Select Task:", task_names)

        if selected_task:
            selected_task_info = next(task for task in tasks if task["recipe_id"] == selected_task)

            # Inside the loop where st.button widgets are created for tasks
            for task in tasks:
                    run_button_key = f"run_button_{task['recipe_id']}"
                    delete_button_key = f"delete_button_{task['recipe_id']}"

                    # Use the unique keys for st.button widgets
                    if st.button(f"Run Task: {selected_task}", key=run_button_key):
                        try:
                            run_spark_job(SPARK_MASTER, selected_task_info['recipe_id'],
                                          selected_task_info['code_path'])
                            st.success(f"Task '{selected_task}' executed successfully!")
                        except Exception as e:
                            st.error(f"Failed to execute task '{selected_task}': {e}")

                        cron_expression = selected_task_info["cron_expression"]
                        start_time = selected_task_info["start_time"]
                        end_time = selected_task_info["end_time"]

                        job = scheduler.add_job(
                            run_spark_job,
                            CronTrigger.from_crontab(cron_expression),
                            args=[SPARK_MASTER, selected_task_info['recipe_id'], selected_task_info['code_path']],
                            start_date=start_time,
                            end_date=end_time
                        )

                    if st.button(f"Delete Task: {selected_task}", key=delete_button_key):
                        try:
                            task_collection.delete_one({"recipe_id": selected_task})
                            st.sidebar.success(f"Task '{selected_task}' deleted successfully!")
                            tasks = fetch_tasks()
                            if not tasks:
                                st.info("No tasks found in the collection.")
                            else:
                                for task in tasks:
                                    task["start_time"] = datetime.strptime(task["start_time"], '%H:%M:%S').time()
                                    task["end_time"] = datetime.strptime(task["end_time"], '%H:%M:%S').time()
                                task_names = [task["recipe_id"] for task in tasks]
                        except Exception as e:
                            st.sidebar.error(f"Error deleting task: {e}")

    # DLQ Section
    st.markdown("<h2 style='text-align:center;color: #FF0000;'>DEAD LETTER QUEUE </h2>", unsafe_allow_html=True)
    dlq_tasks = fetch_dlq_tasks()

    if not dlq_tasks:
        st.info("No tasks found in the DLQ.")
    else:
        for dlq_task in dlq_tasks:
            retry_button_key = f"retry_button_{dlq_task['recipe_id']}"
            delete_dlq_button_key = f"delete_dlq_button_{dlq_task['recipe_id']}"

            num_retries = st.number_input("Number of Retries", min_value=1, max_value=10, step=1, value=3)

            if st.button(f"Retry Task: {dlq_task['recipe_id']}", key=retry_button_key):
                retry_dlq_task(dlq_task, num_retries)
            if st.button(f"Delete DLQ Task: {dlq_task['recipe_id']}", key=delete_dlq_button_key):
                try:
                    _, _, _, dlq_collection = connect_to_mongodb(MONGO_CONNECTION_STRING)
                    dlq_collection.delete_one({"_id": dlq_task["_id"]})
                    st.success(f"DLQ Task '{dlq_task['recipe_id']}' deleted successfully!")
                except Exception as e:
                    st.error(f"Error deleting DLQ task: {e}")

    atexit.register(on_server_shutdown)

if __name__ == "__main__":
    main()
