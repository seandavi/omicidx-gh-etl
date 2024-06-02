import logging

logging.basicConfig(level=logging.INFO)

formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

# create a stream handler
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)


def get_logger(name: str = "main", level: int = logging.INFO):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.addHandler(stream_handler)
    return logger
