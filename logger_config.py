import logging
from logging.handlers import RotatingFileHandler

log_file = 'datahub/application.log'

# Create a rotating file handler
file_handler = RotatingFileHandler(log_file, maxBytes=5_000_000, backupCount=5)
file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(filename)s - %(message)s')
file_handler.setFormatter(file_formatter)

# Create a console handler (to also print logs)
console_handler = logging.StreamHandler()
console_formatter = logging.Formatter('%(levelname)s - %(message)s')  # Simpler format for console
console_handler.setFormatter(console_formatter)

# Create logger instance
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(file_handler)  # Write logs to file
logger.addHandler(console_handler)  # Print logs to console