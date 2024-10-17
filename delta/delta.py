import asyncio
import logging
import uuid

import pandas as pd
from globus_compute_sdk import Client, Executor
from globus_compute_sdk.serialize import CombinedCode, DillDataBase64

from .global_table import GlobalTable
from .scheduler import Scheduler
from .task_handler import TaskHandler
from .task_tracker import TaskTracker
from .tasks import get_count


class Delta:
    def __init__(self, endpoints, interactive=False):
        self.endpoints = endpoints
        self.endpoint_uuids = list(self.endpoints.values())
        self.global_table = GlobalTable(
            interactive=interactive, endpoints=self.endpoint_uuids
        )
        self.client = Client(
            code_serialization_strategy=CombinedCode(),
            data_serialization_strategy=DillDataBase64(),
        )
        self.handler = TaskHandler(self.client)
        self.tracker = TaskTracker()
        self.scheduler = Scheduler(self.global_table)
        self.executors = {}

        # Run the asynchronous initialization
        asyncio.run(self._async_init())

    async def _async_init(self):
        """Asynchronous initialization method."""
        self.executors = await self._initialize_executors()
        await self._wake_up_endpoints()

    async def _initialize_executors(self):
        """
        Initializes Globus Compute Executors for each endpoint.

        Returns:
        - dict: Mapping from endpoint names to Executor instances.
        """
        self.client = Client(
            code_serialization_strategy=CombinedCode(),
            data_serialization_strategy=DillDataBase64(),
        )
        executors = {}
        for name, uuid in self.endpoints.items():
            executors[name] = Executor(
                endpoint_id=uuid,
                client=self.client,
                user_endpoint_config={
                    "worker_init": "conda activate delta",
                    "endpoint_setup": "",
                    "max_workers": 1,  # Default value; will be updated later
                },
            )
        return executors

    def _launch_get_count_tasks(self):
        """
        Launches get_count tasks for each endpoint to gather CPU counts.
        """
        for ep_name, executor in self.executors.items():
            future = self.handler.submit_task(executor, get_count, args=(True,))
            self.tracker.add_task(task_id=f"get_count_{ep_name}", future=future)

    def _process_get_count_results(self):
        """
        Processes the results of get_count tasks and updates the observations.

        Raises:
        - ValueError: If any get_count task fails.
        """
        completed = self.tracker.get_completed_tasks()
        cpu_counts = {}  # Dictionary to store CPU counts
        for task_id, result in completed:
            if "error" in result:
                raise ValueError(f"Error in {task_id}: {result['error']}")
            # Extract endpoint name from task_id
            ep_name = task_id.replace("get_count_", "")
            # Map endpoint name to UUID
            ep_uuid = self._get_uuid_by_name(ep_name)
            if not ep_uuid:
                raise ValueError(f"Unknown endpoint name: {ep_name}")
            cpu_count = result["result"]
            cpu_counts[ep_uuid] = cpu_count
            # Update observations
            if "get_count" not in self.global_table.observations.index:
                self.global_table.observations = self.global_table.observations._append(
                    pd.Series(name="get_count")
                )
            self.global_table.observations.at["get_count", ep_uuid] = cpu_count
        self.global_table.save_table()
        return cpu_counts  # Return the CPU counts for updating executors

    def _initialize_or_load_global_table(self):
        """
        Initializes or loads the global table, ensuring get_count tasks are completed.
        """
        # Check if 'get_count' exists in the observations; if not, initialize
        if "get_count" not in self.global_table.observations.index:
            self.global_table.observations = self.global_table.observations._append(
                pd.Series(name="get_count")
            )
            self.global_table.save_table()

        # Launch get_count tasks for endpoints without observations
        missing_counts = [
            ep_uuid
            for ep_name, ep_uuid in self.endpoints.items()
            if pd.isna(self.global_table.observations.at["get_count", ep_uuid])
        ]
        if missing_counts:
            for ep_uuid in missing_counts:
                # Find endpoint name by UUID
                ep_name = self._get_name_by_uuid(ep_uuid)
                if not ep_name:
                    raise ValueError(f"Unknown endpoint UUID: {ep_uuid}")
                executor = self.executors[ep_name]
                future = self.handler.submit_task(executor, get_count, args=())
                self.tracker.add_task(task_id=f"get_count_{ep_name}", future=future)

        # Wait for all get_count tasks to complete and collect CPU counts
        cpu_counts = {}
        while True:
            completed = self.tracker.get_completed_tasks()
            if not completed and not missing_counts:
                break
            if completed:
                counts = self._process_get_count_results()
                cpu_counts.update(counts)
        return cpu_counts  # Return the collected CPU counts

    def _get_uuid_by_name(self, name):
        """
        Retrieves the UUID for a given endpoint name.

        Parameters:
        - name (str): The name of the endpoint.

        Returns:
        - str or None: The UUID of the endpoint, or None if not found.
        """
        return self.endpoints.get(name)

    def _get_name_by_uuid(self, uuid):
        """
        Retrieves the endpoint name for a given UUID.

        Parameters:
        - uuid (str): The UUID of the endpoint.

        Returns:
        - str or None: The name of the endpoint, or None if not found.
        """
        for name, ep_uuid in self.endpoints.items():
            if ep_uuid == uuid:
                return name
        return None

    def update_executors(self, cpu_counts):
        """
        Updates the max_workers for each executor based on CPU counts.

        Parameters:
        - cpu_counts (dict): Dictionary mapping endpoint UUIDs to their CPU counts.

        Returns:
        - dict: Updated executors with new max_workers.
        """
        for ep_uuid, cpu_count in cpu_counts.items():
            ep_name = self._get_name_by_uuid(ep_uuid)
            if not ep_name:
                print(f"Unknown endpoint UUID: {ep_uuid}. Skipping executor update.")
                continue
            executor = self.executors.get(ep_name)
            if executor:
                try:
                    # Update the max_workers in the executor's user_endpoint_config
                    executor.user_endpoint_config["max_workers"] = cpu_count
                    print(
                        f"Updated max_workers for executor '{ep_name}' to {cpu_count}."
                    )
                except Exception as e:
                    print(f"Failed to update executor '{ep_name}': {e}")
            else:
                print(f"Executor for endpoint '{ep_name}' not found.")

        return self.executors  # Return the updated executors

    async def run(self, tasks):
        """
        Runs the Delta system: schedules tasks, submits them, and collects results.

        Parameters:
        - tasks (list): List of tuples, each containing (function, args).

        Returns:
        - dict: Mapping from task IDs to their results.
        """
        # Update predictions based on observations
        self.scheduler.update_predictions()

        # Assign unique IDs to tasks
        task_dicts = [
            {"id": str(uuid.uuid4()), "function": func, "args": args}
            for func, args in tasks
        ]

        # Schedule tasks
        placements = self.scheduler.schedule_tasks(task_dicts)

        # Submit tasks based on scheduler placement
        for task in task_dicts:
            endpoint_uuid = placements.get(task["id"])
            if not endpoint_uuid:
                logging.warning(f"No placement found for task {task['id']}")
                continue
            ep_name = self._get_name_by_uuid(endpoint_uuid)
            if not ep_name:
                logging.warning(
                    f"Unknown endpoint UUID: {endpoint_uuid} for task {task['id']}"
                )
                continue
            executor = self.executors.get(ep_name)
            if executor:
                future = self.handler.submit_task(
                    executor, task["function"], args=task["args"]
                )
                self.tracker.add_task(task_id=task["id"], future=future)

        # Collect results
        results = {}
        while self.tracker.tasks:
            completed = await self.tracker.get_completed_tasks()
            for task_id, result in completed:
                if "error" in result:
                    logging.error(
                        f"Task {task_id} failed with error: {result['error']}"
                    )
                    results[task_id] = None
                else:
                    results[task_id] = result["result"]
            await asyncio.sleep(0.1)  # Short sleep to prevent tight loop
        return results

    async def _wake_up_endpoints(self):
        """
        Sends 'get_count' tasks to all endpoints and waits for responses.
        Updates executors for responsive endpoints after 5 seconds.
        """
        wake_up_tasks = []
        for ep_name, executor in self.executors.items():
            future = self.handler.submit_task(executor, get_count, args=())
            self.tracker.add_task(task_id=f"get_count_{ep_name}", future=future)
            wake_up_tasks.append(future)

        # Wait for 5 seconds
        await asyncio.sleep(5)

        # Process results for responsive endpoints
        responsive_endpoints = []
        for ep_name, future in zip(self.executors.keys(), wake_up_tasks):
            if future.done():
                result = self.handler.unwrap_result(future)
                if "error" not in result:
                    responsive_endpoints.append(ep_name)
                    cpu_count = result["result"]
                    self._update_executor(ep_name, cpu_count)
                    self._update_global_table(ep_name, cpu_count)

        # Continue waiting for other endpoints
        remaining_tasks = [task for task in wake_up_tasks if not task.done()]
        if remaining_tasks:
            logging.info(f"Waiting for {len(remaining_tasks)} endpoints to respond...")
            for future in asyncio.as_completed(remaining_tasks, timeout=None):
                result = await future
                ep_name = self._get_endpoint_name_from_future(future)
                if "error" not in result:
                    cpu_count = result["result"]
                    self._update_executor(ep_name, cpu_count)
                    self._update_global_table(ep_name, cpu_count)
                    logging.info(
                        f"Endpoint {ep_name} came online with {cpu_count} CPUs."
                    )

    def _update_executor(self, ep_name, cpu_count):
        """Update the executor's max_workers based on CPU count."""
        self.executors[ep_name].user_endpoint_config["max_workers"] = cpu_count

    def _update_global_table(self, ep_name, cpu_count):
        """Update the global table with the new CPU count."""
        ep_uuid = self._get_uuid_by_name(ep_name)
        if "get_count" not in self.global_table.observations.index:
            self.global_table.observations = self.global_table.observations._append(
                pd.Series(name="get_count")
            )
        self.global_table.observations.at["get_count", ep_uuid] = cpu_count
        self.global_table.save_table()

    def _get_endpoint_name_from_future(self, future):
        """Get the endpoint name associated with a future."""
        for task_id, task_future in self.tracker.tasks.items():
            if task_future == future:
                return task_id.replace("get_count_", "")
        return None
