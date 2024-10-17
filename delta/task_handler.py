from functools import wraps
from time import time

from globus_compute_sdk import Client, Executor


class TaskHandler:
    def __init__(self, client: Client):
        self.client = client

    def wrap_function(self, fn):
        """
        Wraps a function to measure its execution time.

        Parameters:
        - fn (callable): The function to wrap.

        Returns:
        - callable: The wrapped function.
        """

        @wraps(fn)
        def wrapped(*args, **kwargs):
            start_time = time()
            result = fn(*args, **kwargs)
            end_time = time()
            execution_time = end_time - start_time
            return {"result": result, "execution_time": execution_time}

        return wrapped

    def submit_task(self, executor: Executor, fn, args):
        """
        Submits a single task to the executor.

        Parameters:
        - executor (Executor): The Globus Compute executor.
        - fn (callable): The function to execute.
        - args (tuple): Arguments to pass to the function.

        Returns:
        - Future: The future object representing the submitted task.
        """
        wrapped_fn = self.wrap_function(fn)
        future = executor.submit(wrapped_fn, *args)
        return future

    def submit_batch(self, executor: Executor, fn, args_list):
        """
        Submits a batch of tasks to the executor.

        Parameters:
        - executor (Executor): The Globus Compute executor.
        - fn (callable): The function to execute.
        - args_list (list): List of argument tuples for each task.

        Returns:
        - list: List of future objects representing the submitted tasks.
        """
        wrapped_fn = self.wrap_function(fn)
        futures = [executor.submit(wrapped_fn, *args) for args in args_list]
        return futures

    def unwrap_result(self, future):
        """
        Retrieves the result and execution time from a future.

        Parameters:
        - future (Future): The future object.

        Returns:
        - dict: Dictionary containing 'result' and 'execution_time'.
        """
        try:
            result = future.result(timeout=60)  # Adjust timeout as needed
            return result
        except Exception as e:
            return {"result": None, "execution_time": None, "error": str(e)}
