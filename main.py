import asyncio
from delta.delta import Delta

# Define endpoints and their configurations
roomba = "8d957140-13fd-4aaa-be49-bacdc40f88ad"
ryerson = "a018676d-48b7-4ddf-bf9c-3d828b8bd7f4"
kestrel = "b9af92f4-7117-4176-97e5-24512b14ef28"
hawfinch = "5c497659-6b8f-4c81-b130-89b17519412f"

endpoints = {
    "hawfinch": hawfinch,
    "kestrel": kestrel,
    "roomba": roomba,
    "ryerson": ryerson,
}


def example_task(x, y):
    return x + y


# Define tasks as a list of tuples (function, args)
tasks = [(example_task, (1, 2)), (example_task, (3, 4))]


async def main():
    # Create a dictionary with UUIDs as keys
    endpoint_config = {
        hawfinch: {
            "worker_init": "conda activate new_delta",
            "endpoint_setup": "",
            "max_workers": 12,
        },
        kestrel: {
            "worker_init": "conda activate delta",
            "endpoint_setup": "",
            "max_workers": 8,
        },
        roomba: {
            "worker_init": "conda activate delta",
            "endpoint_setup": "",
            "max_workers": 8,
        },
        ryerson: {
            "worker_init": "conda activate delta",
            "endpoint_setup": "",
            "max_workers": 8,
        },
    }

    delta = Delta(endpoints, interactive=False)
    results = await delta.run(tasks)
    print(results)


if __name__ == "__main__":
    asyncio.run(main())
