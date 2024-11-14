from prefect import flow


@flow(log_prints=True)
def my_flow():

    data = {
        "client_id": "BpKWanZRLGRzoEPYH",
        "datasource_id": "brevo",
        "profile_id": "111111111",
        "access_token": "111111111",
        "start_date": "2023-11-01T00:00:00.000Z",
    }

    print(f"I'm a flow from a GitHub repo! And I've changed!: {data}")
