"""
pypedream.logging contains utilities for logging in pypedream.

these wrappers all get the logger from the context, which is a lookup from a contextvar.

this is a good idea, but it's not always the best idea. if you're logging in a hot loop, you don't want to be doing a
context lookup every time you log. i mean, this is a library for building pipelines, so it's not like your bottleneck
is going to be a lookup, just dont do it in a hot loop.

"""

from pypedream import ctx

logger = ctx.logger


def info(msg: str, *args, **kwargs):
    """
    log an info message
    """
    logger().info(msg, *args, **kwargs)


def debug(msg: str, *args, **kwargs):
    """
    log a debug message
    """
    logger().debug(msg, *args, **kwargs)


def warning(msg: str, *args, **kwargs):
    """
    log a warning message
    """
    logger().warning(msg, *args, **kwargs)


def error(msg: str, *args, **kwargs):
    """
    log an error message
    """
    logger().error(msg, *args, **kwargs)


def critical(msg: str, *args, **kwargs):
    """
    log a critical message
    """
    logger().critical(msg, *args, **kwargs)


def exception(msg: str, *args, **kwargs):
    """
    log an exception message
    """
    logger().exception(msg, *args, **kwargs)


def log(level, msg: str, *args, **kwargs):
    """
    log a message at the specified level
    """
    logger().log(level, msg, *args, **kwargs)


__all__ = [
    "logger",
    "info",
    "debug",
    "warning",
    "error",
    "critical",
    "exception",
    "log",
]
