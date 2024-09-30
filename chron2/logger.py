import logging
from datetime import datetime

class PipelineLogger:
    def __init__(self, log_file="pipeline.log"):
        # Create a custom logger
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)
        
        # Accumulate log messages in a list
        self.log_messages = []

        # Create handlers
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.DEBUG)

        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)

        # Create formatters and add them to handlers
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)

        # Add handlers to the logger
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)

    def info(self, message):
        self.logger.info(message)
        self.log_messages.append(f"INFO: {message}")

    def debug(self, message):
        self.logger.debug(message)
        self.log_messages.append(f"DEBUG: {message}")

    def error(self, message):
        self.logger.error(message)
        self.log_messages.append(f"ERROR: {message}")

    def warn(self, message):
        self.logger.warning(message)
        self.log_messages.append(f"WARNING: {message}")

    def get_logs(self):
        return "\n".join(self.log_messages)
