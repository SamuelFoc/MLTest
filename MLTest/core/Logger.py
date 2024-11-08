import logging

class LoggerSingleton:
    """
    A singleton logger class for centralized logging management.
    """
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(LoggerSingleton, cls).__new__(cls)
            cls._instance._setup_logger()
        return cls._instance

    def _setup_logger(self):
        """
        Configures the logger instance.
        """
        self.logger = logging.getLogger("CentralLogger")
        self.logger.setLevel(logging.INFO)
        if not self.logger.hasHandlers():
            handler = logging.StreamHandler()
            formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)

    def log(self, message: str, level: str = "INFO"):
        """
        Logs a message at the specified level.

        Parameters:
        - message (str): The message to log.
        - level (str): The logging level ('INFO', 'DEBUG', 'WARNING', 'ERROR', 'CRITICAL').
        """
        log_method = getattr(self.logger, level.lower(), self.logger.info)
        log_method(message)