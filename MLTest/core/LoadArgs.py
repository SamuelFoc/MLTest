import os

def load_args(file_path: str, variable_name: str):
    """
    Loads a specific variable from a Python file.

    Parameters:
    - file_path (str): Path to the Python file.
    - variable_name (str): Name of the variable to load.

    Returns:
    - The value of the variable if it exists in the file.

    Raises:
    - FileNotFoundError: If the file does not exist.
    - KeyError: If the variable is not defined in the file.
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"The file '{file_path}' does not exist.")

    # Create a controlled namespace
    namespace = {}
    with open(file_path, 'r') as file:
        exec(file.read(), namespace)

    # Attempt to retrieve the variable
    if variable_name not in namespace:
        raise KeyError(f"The variable '{variable_name}' is not defined in '{file_path}'.")

    return namespace[variable_name]