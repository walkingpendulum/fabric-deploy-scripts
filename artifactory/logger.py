import logging


def make_logger(logger_name='artifactory-cli', level=logging.INFO, fmt=None):
    logger = logging.getLogger(logger_name)
    logger.setLevel(level)

    handler_name = 'artifactory_cli_default_stream_handler'
    if handler_name not in {handler.name for handler in logger.handlers}:
        ch = logging.StreamHandler()
        ch.setLevel(level)

        fmt = fmt or '%(threadName)s %(levelname)s: %(message)s'
        formatter = logging.Formatter(fmt)
        ch.setFormatter(formatter)
        ch.set_name(handler_name)

        logger.addHandler(ch)

    return logger
