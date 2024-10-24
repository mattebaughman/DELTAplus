import numpy as np
import pandas as pd


class Scheduler:
    def __init__(self, global_table, strategy="delta"):
        self.global_table = global_table
        self.strategies = {
            "delta": self.delta_schedule,
            "thread": self.heuristic_thread_schedule,
            "compute": self.heuristic_compute_schedule,
            "round_robin": self.round_robin_schedule,
            "weighted_round_robin": self.weighted_round_robin_schedule,
            "least_loaded_first": self.least_loaded_first_schedule,
            "fastest_endpoint": self.fastest_endpoint_schedule,
        }
        if strategy not in self.strategies:
            raise ValueError(
                f"Invalid strategy: {strategy}. Valid options are: {list(self.strategies.keys())}"
            )
        self.current_strategy = self.strategies[strategy]

    def update_predictions(self):
        if not self.global_table.predictions.empty:
            self.global_table.predictions.fillna(
                self.global_table.predictions.mean(axis=1), inplace=True
            )
            self.global_table.save_table()

    def schedule_tasks(self, tasks: list):
        return self.current_strategy(tasks)

    def delta_schedule(self, tasks: list):
        placement = {}
        for task in tasks:
            function_name = task["function"].__name__
            if function_name in self.global_table.predictions.index:
                probabilities = self.global_table.predictions.loc[function_name].values
                endpoints = self.global_table.predictions.columns
                endpoint = np.random.choice(endpoints, p=probabilities)
            else:
                probabilities = self.global_table.predictions.mean(axis=0).values
                endpoints = self.global_table.predictions.columns
                probabilities = probabilities / probabilities.sum()
                endpoint = np.random.choice(endpoints, p=probabilities)
            placement[task["id"]] = endpoint
        return placement

    def heuristic_thread_schedule(self, tasks: list):
        placement = {}
        core_counts = self.global_table.observations.loc["get_count"]
        total_cores = core_counts.sum()

        if total_cores == 0:
            return self.delta_schedule(tasks)

        endpoints = core_counts.index
        probabilities = core_counts.values / total_cores

        for task in tasks:
            endpoint = np.random.choice(endpoints, p=probabilities)
            placement[task["id"]] = endpoint

        return placement

    def heuristic_compute_schedule(self, tasks: list):
        placement = {}
        try:
            flops = (
                self.global_table.observations.loc["get_count"]["count"]
                * self.global_table.observations.loc["get_count"]["max_freq"]
                * 2
            )
            total_flops = flops.sum()
            if total_flops == 0:
                return self.delta_schedule(tasks)
            probabilities = flops / total_flops
            endpoints = flops.index
            for task in tasks:
                endpoint = np.random.choice(endpoints, p=probabilities)
                placement[task["id"]] = endpoint
            return placement
        except (KeyError, AttributeError):
            return self.delta_schedule(tasks)

    def round_robin_schedule(self, tasks: list):
        placement = {}
        endpoints = self.global_table.predictions.columns
        task_index = 0
        for task in tasks:
            endpoint = endpoints[task_index % len(endpoints)]
            placement[task["id"]] = endpoint
            task_index += 1
        return placement

    def weighted_round_robin_schedule(self, tasks: list):
        placement = {}
        core_counts = self.global_table.observations.loc["get_count"]
        if core_counts.sum() == 0:
            return self.delta_schedule(tasks)

        endpoints = []
        for endpoint, count in core_counts.items():
            weight = max(1, int(count))
            endpoints.extend([endpoint] * weight)

        if not endpoints:
            return self.delta_schedule(tasks)

        for i, task in enumerate(tasks):
            endpoint = endpoints[i % len(endpoints)]
            placement[task["id"]] = endpoint

        return placement

    def least_loaded_first_schedule(self, tasks: list):
        placement = {}
        core_counts = self.global_table.observations.loc["get_count"]
        if core_counts.sum() == 0:
            return self.delta_schedule(tasks)

        active_tasks = {endpoint: 0 for endpoint in core_counts.index}

        for task in tasks:
            load_ratios = {}
            for endpoint in core_counts.index:
                cpu_count = core_counts[endpoint]
                if cpu_count > 0:
                    load_ratios[endpoint] = active_tasks[endpoint] / cpu_count
                else:
                    load_ratios[endpoint] = float("inf")

            selected_endpoint = min(load_ratios.items(), key=lambda x: x[1])[0]
            placement[task["id"]] = selected_endpoint
            active_tasks[selected_endpoint] += 1

        return placement

    def fastest_endpoint_schedule(self, tasks: list):
        placement = {}
        try:
            flops = (
                self.global_table.observations.loc["get_count"]["count"]
                * self.global_table.observations.loc["get_count"]["max_freq"]
                * 2
            )
            if flops.sum() == 0:
                return self.delta_schedule(tasks)

            fastest_endpoint = flops.idxmax()

            for task in tasks:
                placement[task["id"]] = fastest_endpoint

            return placement
        except (KeyError, AttributeError):
            return self.delta_schedule(tasks)
