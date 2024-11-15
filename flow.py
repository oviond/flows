from prefect import flow, task
from prefect.task_runners import ThreadPoolTaskRunner
from prefect.logging import get_run_logger
import traceback
import requests
import os

@task(log_prints=True)
def run_https_request(data):
    logger = get_run_logger()
    logger.info(f"Initiating HTTPS request with data: {data}")
    url = "http://acck0ssowk0s84sss8ockk40.54.90.238.205.sslip.io/run/pipeline"
    try:
        response = requests.post(url, json=data, verify=False)
        response.raise_for_status()
        logger.info(f"Received response: {response.json()}")
        return response.json()
    except requests.exceptions.HTTPError as http_err:
        logger.error(f"HTTP error occurred: {http_err}")
        raise
    except Exception as err:
        logger.error(f"An error occurred: {err}")
        raise

@flow(log_prints=True, task_runner=ThreadPoolTaskRunner(max_workers=10))
def elt_flow():

    client_id = os.getenv("CLIENT_ID")
    datasource_id = os.getenv("DATASOURCE_ID")
    profile_id = os.getenv("PROFILE_ID")
    access_token = os.getenv("ACCESS_TOKEN")
    start_date = os.getenv("START_DATE")
    
    data = {
        "client_id": client_id,
        "datasource_id": datasource_id,
        "profile_id": profile_id,
        "access_token": access_token,
        "start_date": start_date,
    }

    logger = get_run_logger()
    logger.info(f"Starting ETL flow with data: {data}")
    try:
        response_future = run_https_request.submit(data)  # Non-blocking
        response = response_future.result()  # Explicitly resolve the future
        logger.info(f"Flow completed successfully with response: {response}")
        return response  # Return the resolved result if needed
    except Exception as e:
        logger.error(f"Flow encountered an error: {e}")
        logger.debug(f"Full traceback: {traceback.format_exc()}")
        raise 
