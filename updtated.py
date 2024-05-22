import logging
import os
from pymongo import MongoClient
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from datetime import datetime
import streamlit as st
import atexit
from pyspark.sql import SparkSession

# Custom MongoDB logging handler
class MongoDBHandler(logging.Handler):
    def __init__(self, connection_string, database_name, collection_name):
        logging.Handler.__init__(self)
        super().__init__()
        client = MongoClient(connection_string)
        self.db = client[database_name]
        self.collection = self.db[collection_name]

    def emit(self, record):
        log_entry = self.format(record)
        log_data = {
            "message": log_entry,
            "level": record.levelname,
            "timestamp": datetime.now()
        }
        self.collection.insert_one(log_data)

# Load configurations
MONGO_CONNECTION_STRING = os.getenv("MONGO_CONNECTION_STRING", "mongodb+srv://Ramees:Ramees@cluster1.vcarrff.mongodb.net/")
SPARK_MASTER = os.getenv("SPARK_MASTER", "spark://0.0.0.0:7077")
MAX_RETRIES = 3

# Configure logging
LOG_DATABASE_NAME = "sparkproject"
LOG_COLLECTION_NAME = "logs"

# Create MongoDB logging handler
mongo_handler = MongoDBHandler(MONGO_CONNECTION_STRING, LOG_DATABASE_NAME, LOG_COLLECTION_NAME)
mongo_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
mongo_handler.setFormatter(formatter)

# Set up logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', filename='DAS.log')
logging.getLogger().addHandler(mongo_handler)

# Initialize the scheduler
scheduler = BackgroundScheduler()

# MongoDB connection function
def connect_to_mongodb(connection_string):
    try:
        client = MongoClient(connection_string)
        database = client["DAS"]
        task_collection = database["Task"]
        failed_task_collection = database["failed_tasks"]
        logging.info("Connected to MongoDB Atlas")
        return client, database, task_collection, failed_task_collection
    except Exception as e:
        logging.error(f"Failed to connect to MongoDB Atlas: {e}")
        raise SystemExit("Failed to connect to MongoDB Atlas. Exiting script.")

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

def fetch_failed_tasks():
    try:
        _, _, _, failed_task_collection = connect_to_mongodb(MONGO_CONNECTION_STRING)
        failed_tasks = list(failed_task_collection.find({}))
        if failed_tasks:
            logging.info("Failed tasks retrieved from the collection")
        else:
            logging.info("No failed tasks found in the collection")
        return failed_tasks
    except Exception as e:
        logging.error(f"Failed to fetch failed tasks from the collection: {e}")
        return []

def run_spark_job(spark_master, recipe_id, script_path, retry_count=0, error_container=None):
    try:
        logging.info(f"Initializing Spark session with master: {spark_master}")
        spark = SparkSession.builder.master(spark_master).getOrCreate()

        logging.info(f"Running Spark job for recipe_id: {recipe_id} using script: {script_path}")
        with open(script_path, 'r') as file:
            script_code = file.read()
        exec(script_code)
        logging.info("Spark job execution successful")
        update_task_status(recipe_id, "success", retry_count)
        spark.stop()
    except Exception as e:
        logging.error(f"Error running Spark job: {e}")
        if retry_count < MAX_RETRIES:
            retry_count += 1
            logging.info(f"Retrying task {recipe_id}, attempt {retry_count}")
            run_spark_job(spark_master, recipe_id, script_path, retry_count, error_container)
        else:
            update_task_status(recipe_id, "failed", retry_count)
            save_failed_task(recipe_id, script_path)
            logging.error(f"Task {recipe_id} failed after {MAX_RETRIES} retries. Stopping periodic job.")
            scheduler.remove_job(f"{recipe_id}_job")
            if error_container:
                error_container.error(f"Task '{recipe_id}' failed execution after {MAX_RETRIES} retries. Please check the log for errors.")
        raise

def update_task_status(recipe_id, status, retry_count):
    try:
        _, _, task_collection, _ = connect_to_mongodb(MONGO_CONNECTION_STRING)
        task_collection.update_one({"recipe_id": recipe_id}, {"$set": {"status": status, "retry_count": retry_count}})
    except Exception as e:
        logging.error(f"Failed to update task status: {e}")

def save_failed_task(recipe_id, script_path):
    try:
        _, _, _, failed_task_collection = connect_to_mongodb(MONGO_CONNECTION_STRING)
        existing_failed_task = failed_task_collection.find_one({"recipe_id": recipe_id})
        if existing_failed_task is None:
            failed_task = {
                "recipe_id": recipe_id,
                "code_path": script_path,
                "status": "failed",
                "timestamp": datetime.now()
            }
            failed_task_collection.insert_one(failed_task)
            logging.info(f"Failed task {recipe_id} saved to failed_tasks collection")
    except Exception as e:
        logging.error(f"Failed to save failed task: {e}")

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
        client, database, task_collection, failed_task_collection = connect_to_mongodb(MONGO_CONNECTION_STRING)
    except SystemExit as e:
        st.error(str(e))
        return

    with st.expander("Important Notice:"):
        st.warning("Please save your work regularly. The server may shut down without warning.")
        st.info("In case of a server shutdown, you may lose unsaved work. We apologize for any inconvenience.")

    scheduler.add_job(notify_status, 'interval', minutes=10)
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
                "end_time": end_time_str,
                "status": "scheduled",
                "retry_count": 0
            }
            task_collection.insert_one(task)
            st.sidebar.success("Task scheduled successfully!")
        except Exception as e:
            st.sidebar.error(f"Error scheduling task: {e}")

    tasks = fetch_tasks()

    if not tasks:
        st.info("No tasks found in the collection.")
    else:
        task_names = [task["recipe_id"] for task in tasks]
        selected_tasks = st.multiselect("Select Tasks:", task_names)

        if selected_tasks:
            if st.button("Run Selected Tasks"):
                for selected_task in selected_tasks:
                    selected_task_info = next(task for task in tasks if task["recipe_id"] == selected_task)
                    error_container = st.container()
                    try:
                        run_spark_job(SPARK_MASTER, selected_task_info['recipe_id'], selected_task_info['code_path'], error_container=error_container)
                        st.success(f"Task '{selected_task}' executed successfully!")
                    except Exception as e:
                        st.error(f"Failed to execute task '{selected_task}': {e}")

                    cron_expression = selected_task_info["cron_expression"]
                    start_time = selected_task_info["start_time"]
                    end_time = selected_task_info["end_time"]

                    job_id = f"{selected_task_info['recipe_id']}_job"
                    scheduler.add_job(
                        run_spark_job,
                        CronTrigger.from_crontab(cron_expression),
                        args=[SPARK_MASTER, selected_task_info['recipe_id'], selected_task_info['code_path']],
                        start_date=start_time,
                        end_date=end_time,
                        id=job_id
                    )

            if st.button("Retry Selected Tasks"):
                for selected_task in selected_tasks:
                    selected_task_info = next(task for task in tasks if task["recipe_id"] == selected_task)
                    error_container = st.container()
                    try:
                        run_spark_job(SPARK_MASTER, selected_task_info['recipe_id'], selected_task_info['code_path'], retry_count=selected_task_info.get("retry_count", 0), error_container=error_container)
                        st.success(f"Retry for task '{selected_task}' initiated successfully!")
                    except Exception as e:
                        st.error(f"Failed to retry task '{selected_task}': {e}")

            if st.button("Delete Selected Tasks"):
                for selected_task in selected_tasks:
                    try:
                        task_collection.delete_one({"recipe_id": selected_task})
                        job_id = f"{selected_task}_job"
                        if scheduler.get_job(job_id):
                            scheduler.remove_job(job_id)
                        st.sidebar.success(f"Task '{selected_task}' deleted successfully!")
                        tasks = fetch_tasks()
                        if not tasks:
                            st.info("No tasks found in the collection.")
                        else:
                            task_names = [task["recipe_id"] for task in tasks]
                    except Exception as e:
                        st.sidebar.error(f"Error deleting task: {e}")

        failed_tasks = fetch_failed_tasks()
        if failed_tasks:
            st.markdown("<h2 style='color: red;'>FAILED TASKS:</h2>", unsafe_allow_html=True)
            for failed_task in failed_tasks:
                st.error(f"Task '{failed_task['recipe_id']}' failed execution after {MAX_RETRIES} retries. Please check the log for errors.")

                if st.button(f"Delete Failed Task: {failed_task['recipe_id']}"):
                    try:
                        failed_task_collection.delete_one({"recipe_id": failed_task['recipe_id']})
                        st.success(f"Failed task '{failed_task['recipe_id']}' deleted successfully!")
                    except Exception as e:
                        st.error(f"Error deleting failed task '{failed_task['recipe_id']}': {e}")

    atexit.register(on_server_shutdown)

if __name__ == "__main__":
        main()