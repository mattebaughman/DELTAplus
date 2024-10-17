import asyncio


class TaskTracker:
    def __init__(self):
        self.tasks = {}

    def add_task(self, task_id, future):
        self.tasks[task_id] = future

    async def get_completed_tasks(self):
        """
        Retrieves and removes completed tasks asynchronously.

        Returns:
        - list: List of tuples containing task_id and result.
        """
        completed = []
        done, _ = await asyncio.wait([fut for fut in self.tasks.values()], timeout=0)
        for future in done:
            task_id = next(tid for tid, fut in self.tasks.items() if fut == future)
            result = future.result()
            completed.append((task_id, result))
            del self.tasks[task_id]
        return completed

    def get_status(self):
        """
        Retrieves the status of all tasks.

        Returns:
        - dict: Dictionary mapping task IDs to their completion status.
        """
        return {tid: fut.done() for tid, fut in self.tasks.items()}
