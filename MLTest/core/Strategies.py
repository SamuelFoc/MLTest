import importlib.util
import os

class UseStrategy:
    def __init__(self, strategies_folder="strategies"):
        self.strategies_folder = strategies_folder

    def load_strategy(self, strategy_name):
        """Dynamically loads a strategy by name."""
        file_path = os.path.join(self.strategies_folder, f"{strategy_name}.py")
        
        if not os.path.isfile(file_path):
            raise FileNotFoundError(f"Strategy file '{strategy_name}.py' not found in '{self.strategies_folder}'")

        # Load the strategy module
        spec = importlib.util.spec_from_file_location(strategy_name, file_path)
        strategy_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(strategy_module)

        # Ensure the strategy file has a `strategy` function defined
        if not hasattr(strategy_module, "strategy"):
            raise ValueError(f"Strategy '{strategy_name}.py' does not contain a 'strategy' function")

        return strategy_module.strategy

    def use(self, strategy_name, data):
        """
        Executes the specified strategy on the provided DataFrame.
        :param strategy_name: Name of the strategy to load.
        :param data: DataFrame to pass into the strategy.
        :return: Processed DataFrame after applying the strategy.
        """
        strategy_func = self.load_strategy(strategy_name)
        return strategy_func(data)