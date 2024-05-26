import datetime
import logging
from os import PathLike
from pathlib import Path
from typing import Any

import structlog
from structlog.dev import ConsoleRenderer
from structlog.processors import (
    CallsiteParameter,
    CallsiteParameterAdder,
    JSONRenderer,
    StackInfoRenderer,
    TimeStamper,
    dict_tracebacks,
)
from structlog.stdlib import (
    BoundLogger as StdBoundLogger,
)
from structlog.stdlib import (
    ExtraAdder,
    PositionalArgumentsFormatter,
    ProcessorFormatter,
    add_log_level,
    add_logger_name,
)
from structlog.contextvars import bound_contextvars

logging_context = bound_contextvars


DEFAULT_SHARED_PROCESSORS: list[structlog.types.Processor] = [
    structlog.contextvars.merge_contextvars,
    add_log_level,
    ProcessorFormatter.wrap_for_formatter,
]

DEFAULT_FILE_PROCESSORS: list[structlog.types.Processor] = [
    ProcessorFormatter.remove_processors_meta,
    dict_tracebacks,
    CallsiteParameterAdder(
        {
            CallsiteParameter.FILENAME,
            CallsiteParameter.FUNC_NAME,
            CallsiteParameter.LINENO,
        }
    ),
    TimeStamper(fmt="iso", utc=False),
    JSONRenderer(),
]

DEFAULT_CONSOLE_PROCESSORS: list[structlog.types.Processor] = [
    ProcessorFormatter.remove_processors_meta,
    PositionalArgumentsFormatter(),
    StackInfoRenderer(),
    TimeStamper(fmt="%Y-%m-%d %H:%M:%S", utc=False),
    add_logger_name,
    ConsoleRenderer(),
]

DEFAULT_CONSOLE_FORMATTER = ProcessorFormatter(
    processors=DEFAULT_CONSOLE_PROCESSORS,
    foreign_pre_chain=[*DEFAULT_SHARED_PROCESSORS, ExtraAdder()],
)

DEFAULT_FILE_FORMATTER = ProcessorFormatter(
    processors=DEFAULT_FILE_PROCESSORS,
    foreign_pre_chain=[*DEFAULT_SHARED_PROCESSORS, ExtraAdder()],
)

structlog.stdlib.recreate_defaults()
BASE_LOGGER = structlog.get_logger("pypedream")


def stdstructlogger(
    name: str,
    stream: bool | Any = True,
    log_dir: Path | PathLike = Path.cwd(),
    file: bool | Path | PathLike | str = True,
    handlers: list[logging.Handler] | None = None,
) -> structlog.stdlib.BoundLogger:
    """
    Get a structlog logger using stdlib loggers with the specified name and configure it.

    Parameters
    ----------
    name : str
        The name of the logger

    stream : bool | Any
        If True, a structlog logger will be created using their rich console logger configured with what I believe are sensible defaults.
        If False, no console logger will be added.
        Otherwise, the value should be an io stream that can be passed into the constructor of a logging.StreamHandler

    log_dir : Path | PathLike
        The directory to write log files to. This is only used if file is True or a string. Defaults to the current working directory.

    file : bool | Path | PathLike | str
        If True, a structlog logger will be created to write json logs to a file named '{name}_{datetime.datetime.now().isoformat()}.log' in the specified log_dir.
        If False, no file logger will be added.
        If it is a Path or PathLike object, the logger will write json logs to the specified file, ignoring the log_dir parameter.
        If it is a string, the logger will write json logs to a file named '{file}' in the specified log_dir.

    handlers : list[logging.Handler] | None
        A list of logging handlers to add to the logger. If None, no additional handlers will be added.
        If not none, this will override the stream and file parameters.


    Returns
    -------
    structlog.stdlib.BoundLogger
        A structlog logger configured with the specified handlers.
    """

    logger = logging.getLogger(name)
    logger.propagate = False
    logger.setLevel(logging.DEBUG)

    if handlers is not None:
        for handler in handlers:
            logger.addHandler(handler)

        return structlog.wrap_logger(logger)

    if stream:
        console_handler = (
            logging.StreamHandler(stream)
            if stream is not True
            else logging.StreamHandler()
        )
        console_handler.setFormatter(DEFAULT_CONSOLE_FORMATTER)
        logger.addHandler(console_handler)

    if file:
        if isinstance(file, (Path, PathLike)):
            file_path = file
        elif isinstance(file, str):
            file_path = Path(log_dir, file)
        else:
            file_path = Path(
                log_dir, f"{name}_{datetime.datetime.now().isoformat()}.log"
            )

        file_handler = logging.FileHandler(str(file_path), mode="a")
        file_handler.setFormatter(DEFAULT_FILE_FORMATTER)
        logger.addHandler(file_handler)

    return structlog.wrap_logger(
        logger, processors=DEFAULT_SHARED_PROCESSORS, wrapper_class=StdBoundLogger
    )


__all__ = [
    "stdstructlogger",
    "logging_context",
    "BASE_LOGGER",
    "DEFAULT_CONSOLE_FORMATTER",
    "DEFAULT_FILE_FORMATTER",
    "DEFAULT_CONSOLE_PROCESSORS",
    "DEFAULT_FILE_PROCESSORS",
    "DEFAULT_SHARED_PROCESSORS",
]
