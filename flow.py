from prefect import flow, task
from prefect.logging import get_run_logger
import traceback

@task(log_prints=True)
def run_https_request(data):
    logger = get_run_logger()
    try:
        logger.info(f"Initiating HTTPS request with data: {data}")
        return True
    except Exception as err:
        logger.error(f"An error occurred: {err}")
        raise

@flow(log_prints=True)
def my_flow():

    data = {
        "client_id": "BpKWanZRLGRzoEPYH",
        "datasource_id": "brevo",
        "profile_id": "111111111",
        "access_token": "111111111",
        "start_date": "2023-11-01T00:00:00.000Z",
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
