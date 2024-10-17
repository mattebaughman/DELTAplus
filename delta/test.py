# test case for delta.py to check if we can submit tasks to globus compute using our current endpoints
from globus_compute_sdk import Client, Executor
from globus_compute_sdk.serialize import CombinedCode
from tasks import get_count

from delta import Delta

# Define endpoints
endpoints = {
    "hawfinch": "5c497659-6b8f-4c81-b130-89b17519412f",
    "kestrel": "b9af92f4-7117-4176-97e5-24512b14ef28",
    "roomba": "8d957140-13fd-4aaa-be49-bacdc40f88ad",
    "ryerson": "a018676d-48b7-4ddf-bf9c-3d828b8bd7f4",
}
gc_client = Client(code_serialization_strategy=CombinedCode())
executor = Executor(
    endpoint_id=endpoints["hawfinch"],
    client=gc_client,
    user_endpoint_config={
        "worker_init": "conda activate delta",
        "endpoint_setup": "",
        "max_workers": 12,  # Default value; will be updated later
    },
)


def example_task(x, y):
    return x + y


fut = executor.submit(get_count, True)
print(fut.result())
