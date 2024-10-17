import asyncio
from delta import Delta

# Define endpoints
endpoints = {
    'hawfinch': '5c497659-6b8f-4c81-b130-89b17519412f',
    'kestrel': 'b9af92f4-7117-4176-97e5-24512b14ef28',
    'roomba': '8d957140-13fd-4aaa-be49-bacdc40f88ad',
    'ryerson': 'a018676d-48b7-4ddf-bf9c-3d828b8bd7f4'
}


def example_task(x, y):
    return x + y

# Define tasks as a list of tuples (function, args)
tasks = [
    (example_task, (1, 2)),
    (example_task, (3, 4))
]

async def main():
    delta = Delta(endpoints)
    results = await delta.run(tasks)
    print(results)

if __name__ == "__main__":
    asyncio.run(main())
