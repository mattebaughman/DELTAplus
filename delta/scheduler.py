import numpy as np
import pandas as pd


class Scheduler:
    def __init__(self, global_table):
        """
        Initializes the Scheduler with the GlobalTable.

        Parameters:
        - global_table (GlobalTable): An instance of the GlobalTable class.
        """
        self.global_table = global_table  # Instance of GlobalTable

    def update_predictions(self):
        """
        Fill empty prediction entries with the mean of non-empty values for each function.
        """
        if not self.global_table.predictions.empty:
            # Assuming each function is a row and endpoints are columns with probabilities
            self.global_table.predictions.fillna(
                self.global_table.predictions.mean(axis=1), inplace=True
            )
            self.global_table.save_table()

    def schedule_tasks(self, tasks: list):
        """
        Schedule tasks based on the predictions in the global table.

        Parameters:
        - tasks (list): List of task dictionaries.

        Returns:
        - placement (dict): Mapping from task IDs to endpoint UUIDs.
        """
        placement = {}
        for task in tasks:
            function_name = task["function"].__name__  # Get function name as string
            if function_name in self.global_table.predictions.index:
                probabilities = self.global_table.predictions.loc[function_name].values
                endpoints = self.global_table.predictions.columns
                endpoint = np.random.choice(endpoints, p=probabilities)
            else:
                probabilities = self.global_table.predictions.mean(axis=0).values
                endpoints = self.global_table.predictions.columns
                # Normalize probabilities to sum to 1
                probabilities = probabilities / probabilities.sum()
                endpoint = np.random.choice(endpoints, p=probabilities)
            placement[task["id"]] = endpoint
        return placement
