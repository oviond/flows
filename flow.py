from prefect import flow, task
from prefect.logging import get_run_logger
import traceback
import requests

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

@flow(log_prints=True)
def my_flow():

    data = {
        "client_id": "BpKWanZRLGRzoEPYH",
        "datasource_id": "shopify",
        "profile_id": "35286a-4",
        "access_token": "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",
        "start_date": "2024-11-13T00:00:00Z",
    }

    logger = get_run_logger()
    logger.info(f"Starting ETL flow with data: {data}")
    try:
        response = run_https_request(data)
        logger.info(f"Flow completed successfully with response: {response}")
    except Exception as e:
        logger.error(f"Flow encountered an error: {e}")
        logger.debug(f"Full traceback: {traceback.format_exc()}")
        raise  # Re-raise the exception to fail the flow with more detailed logs
