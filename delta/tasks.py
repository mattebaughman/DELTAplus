from time import time

import numpy as np


def example_task(x, y):
    """
    Example task that adds two numbers after a delay.

    Parameters:
    - x (int/float): First number.
    - y (int/float): Second number.

    Returns:
    - int/float: The sum of x and y.
    """
    time.sleep(2)
    return x + y


def do_a_test(func_args):
    """
    Function to perform a computational test.

    Parameters:
    - func_args (tuple): A tuple containing (k, t).

    Returns:
    - str: The runtime as a string.
    """
    k, t = func_args
    r = time()
    for n in range(t):
        np.random.random((k, k)).astype("float32") @ np.random.random((k, k)).astype(
            "float32"
        )
    s = time()
    return str(s - r)


def get_count(k=True):
    """
    Function to get CPU count.

    Parameters:
    - k (bool): Unused parameter.

    Returns:
    - int: Number of CPUs.
    """
    import os

    return os.cpu_count()
