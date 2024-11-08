from MLTest.interfaces.Components import FlowComponent
from MLTest.interfaces.Typing import DF
from MLTest.core.Logger import LoggerSingleton


class Logger(FlowComponent):
    """
    A standalone Logger component for logging messages in the pipeline.
    """
    def __init__(self, message: str, level: str = "INFO", log: bool = False):
        """
        Initializes the Logger component.

        Parameters:
        - message (str): The message to log.
        - level (str): The logging level ('INFO', 'DEBUG', 'WARNING', 'ERROR', 'CRITICAL').
        """
        super().__init__(log)
        self.message = message
        self.level = level
        self.logger = LoggerSingleton().logger

    def use(self, data: DF) -> DF:
        """
        Logs the message and returns the unchanged DataFrame.

        Parameters:
        - data (DF): The input DataFrame.

        Returns:
        - DF: The unchanged DataFrame.
        """
        self.log(self.message, self.level)
        return data