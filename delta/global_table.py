import os

import pandas as pd


class GlobalTable:
    def __init__(
        self,
        config_path=os.path.expanduser("~/.delta/"),
        interactive=False,
        endpoints=None,
    ):
        """
        Initializes the GlobalTable.

        Parameters:
        - config_path (str): Path to the configuration directory.
        - interactive (bool): If True, allows interactive prompts for adding endpoints.
        - endpoints (list): List of endpoint UUIDs to initialize the tables without prompts.
        """
        self.config_path = config_path
        self.interactive = interactive
        self.endpoints = endpoints or []  # List of endpoint UUIDs
        self.predictions_path = os.path.join(self.config_path, "predictions.csv")
        self.observations_path = os.path.join(self.config_path, "observations.csv")
        self.predictions = self.initialize_predictions()
        self.observations = self.initialize_observations()

    def initialize_predictions(self):
        try:
            predictions = pd.read_csv(self.predictions_path, index_col=0)
            if self.interactive:
                self.add_endpoints(predictions, data_type="predictions")
            return predictions
        except FileNotFoundError:
            return self.create_new_table(data_type="predictions")

    def initialize_observations(self):
        try:
            observations = pd.read_csv(self.observations_path, index_col=0)
            if self.interactive:
                self.add_endpoints(observations, data_type="observations")
            return observations
        except FileNotFoundError:
            return self.create_new_table(data_type="observations")

    def add_endpoints(self, table, data_type="predictions"):
        if not self.interactive:
            return  # Do not prompt in non-interactive mode
        choice = (
            input("Would you like to add endpoint UUIDs? (y or n): ").strip().lower()
        )
        if choice == "y":
            new_eps = input(
                "Please input Globus Compute endpoint UUIDs separated by commas: "
            ).split(",")
            new_eps = [ep.strip() for ep in new_eps if ep.strip()]
            for ep in new_eps:
                if data_type == "predictions":
                    table[ep] = 1.0  # Use float for predictions
                else:
                    table[ep] = 0.0  # Use float for observations
            table.to_csv(
                self.predictions_path
                if data_type == "predictions"
                else self.observations_path
            )
            if data_type == "predictions":
                self.predictions = table
            else:
                self.observations = table

    def create_new_table(self, data_type="predictions"):
        if self.interactive:
            new_eps = input(
                "Please input Globus Compute endpoint UUIDs separated by commas: "
            ).split(",")
            new_eps = [ep.strip() for ep in new_eps if ep.strip()]
        else:
            new_eps = self.endpoints
        if not new_eps:
            raise ValueError("No endpoints provided and interactive mode is False.")

        if data_type == "predictions":
            # Create a DataFrame with proper numeric dtype
            table = pd.DataFrame(
                1.0,  # Initial value
                index=["example_task"],  # Initial function name
                columns=new_eps,  # Endpoint UUIDs as columns
                dtype=float,  # Explicitly set dtype to float
            )
        else:
            # For observations, create DataFrame with float dtype
            table = pd.DataFrame(
                0.0,  # Initial value of 0 for counts
                index=["get_count"],  # Initial metric
                columns=new_eps,  # Endpoint UUIDs as columns
                dtype=float,  # Explicitly set dtype to float
            )

        os.makedirs(self.config_path, exist_ok=True)
        table.to_csv(
            self.predictions_path
            if data_type == "predictions"
            else self.observations_path
        )

        if data_type == "predictions":
            self.predictions = table
        else:
            self.observations = table
        return table

    def save_table(self):
        self.predictions.to_csv(self.predictions_path)
        self.observations.to_csv(self.observations_path)
