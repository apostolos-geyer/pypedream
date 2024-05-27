import contextvars
import logging
from collections.abc import MutableMapping
from typing import Any, Callable, Iterable, ParamSpec, TypeVar
from os import PathLike
from pathlib import Path
import structlog

from attrs import define, field, Factory
from pypedream.core import context
from pypedream.core.logs import stdstructlogger, logging_context
from pypedream.core.stages import (
    Stage,
    StageInputs,
    StageTable,
)

__all__ = [
    "Pipeline",
    "Parameters",
    "Variables",
    "ExitPipeline",
]

P = ParamSpec("P")
R = TypeVar("R")


class ExitPipeline(Exception):
    """
    Exception to exit the pipeline
    """

    def __init__(self, message: str, *args, error: bool = False):
        self.message = message
        self.error = error
        super().__init__(*args)


class InvalidParameterException(KeyError):
    """
    Exception for trying to do something with a parameter that is not in the parameter set
    """


class UndefinedParameterException(KeyError):
    """
    Exception for trying to do something with a parameter that is not in the parameter set
    """


class UndefinedVariableException(KeyError):
    """
    Exception for trying to do something with a variable that is not in the variable set
    """


@define
class Parameters(MutableMapping[str, Any]):
    UNSET_PARAMETER = "UNSET_PARAMETER"
    """
    A class to manage the parameters of a pipeline

    Ideally parameters are never set directly after the pipeline is initialized and values that need to be modified
    during a pipeline run are modified via Variables (not implemented but yk... ideally)

    Attributes
    ----------
    parameter_set: Iterable[str]
        a set of allowed parameter names

    parameters: dict[str, Any]
        a dictionary of parameter names and values

    defaults: dict[str, Any]
        a dictionary of default values for the parameters

    Methods
    -------
    define(parameter_set: Iterable[str], **defaults: dict) -> Parameters
        Define a Parameters object with the given parameter set and default values

    set(name: str, value: Any) -> None
        sets a parameter in the pipeline if it is in the parameter set, otherwise raises an error

    sets(**kwargs: Any) -> None
        sets multiple parameters in the pipeline

    get(name: str, must: bool = False) -> Any
        gets a parameter from the pipeline.
        If the parameter is not in the parameter set it will error.
        If the parameter is not found, it will return UNSET_PARAMETER or error if must is True

    reset()
        Resets the values of all parameters, does not modify the parameter set.

    __getitem__(name: str) -> Any
        identical to get with dict like access but must is always false and
        will return UNSET_PARAMETER if the parameter is not found

    __setitem__(name: str, value: Any) -> None
        identical to set with dict like access
    """
    parameter_set: Iterable[str] = field(factory=set)
    parameters: dict[str, Any] = field(factory=dict)
    defaults: dict[str, Any] = field(default={})

    @classmethod
    def define(
        cls, parameter_set: Iterable[str] | None = None, **defaults: Any
    ) -> "Parameters":
        """
        Define a Parameters object with the given parameter set and default values

        Parameters
        ----------
        parameter_set: Iterable[str]
            the parameter set to use

        defaults: dict
            default values for the parameters

        Returns
        -------
        a new Parameters object. The returned Parameters will have a parameter_set
        that is the union of any of the values provided in the `parameter_set` argument, and the keys
        of the `defaults` dictionary. The values of the `defaults` will be used as the default values
        for the parameters when `reset` is used.

        """
        parameter_set = parameter_set or set()
        parameter_set = set(parameter_set).union(set(defaults.keys()))
        pp = cls(parameter_set=parameter_set, defaults=defaults)
        pp.sets(**defaults)
        return pp

    def set(self, name: str, value: Any) -> None:
        """
        sets a parameter in the pipeline

        :param name: the name of the parameter
        :param value: the value of the parameter
        """
        if name not in self.parameter_set:
            raise InvalidParameterException(
                f"Attempting to set parameter {name} not found in pipeline parameters"
            )

        self.parameters[name] = value

    def sets(self, **kwargs: Any) -> None:
        """
        sets multiple parameters in the pipeline

        :param kwargs: a dictionary of parameters and values
        """
        if any(name not in self.parameter_set for name in kwargs):
            raise InvalidParameterException(
                f"Attempting to set parameters {[name for name in kwargs if name not in self.parameter_set]} not found in pipeline parameters"
            )

        self.parameters.update(kwargs)

    def get(self, name: str, default=None, must: bool = False) -> Any:
        """
        gets a parameter from the pipeline

        :param name: the name of the parameter
        :param must: whether to raise an error if the parameter is not found
        :returns: the value of the parameter
        """
        if name not in self.parameter_set:
            raise InvalidParameterException(
                f"Attempting to retrieve undeclared parameter {name} from pipeline parameters"
            )
        match (
            value := self.parameters.get(
                name,
                (check := (default if default is not None else self.UNSET_PARAMETER)),
            ),
            must,
        ):
            case (v, True) if v == check:
                raise UndefinedParameterException(
                    f"Declared paramater {name} has not been defined"
                )
            case (_, _):
                return value

    def __getitem__(self, name: str) -> Any:
        return self.get(name, must=True)

    def __setitem__(self, name: str, value: Any) -> None:
        self.set(name, value)

    def __delitem__(self, name: str) -> None:
        del self.parameters[name]

    def __iter__(self) -> Iterable[str]:
        return iter(self.parameters)

    def __len__(self) -> int:
        return len(self.parameters)

    def reset(self):
        """
        Clears the values of all parameters, does not modify the parameter set.
        """
        self.parameters = self.PARAMATERS_FACTORY()
        self.sets(**self.defaults)


@define
class Variables(MutableMapping[str, Any]):
    UNSET_VARIABLE = "UNSET_VARIABLE"
    """
    A class to manage the variables of a pipeline. Variables are values that can be modified during a pipeline run and are
    not set directly in the pipeline parameters. There is no "variable set" because variables can be added and removed at any time.
    If a variable is not found, it will return UNSET_VARIABLE or error if must is True

    Attributes
    ----------
    variables: dict[str, Any]
        a dictionary of variable names and values

    defaults: dict[str, Any]
        a dictionary of default values for the variables

    Methods
    -------
    define(**defaults: Any) -> Variables
        Define a Variables object with the given default values

    set(name: str, value: Any) -> None
        sets a variable in the pipeline

    sets(**kwargs: Any) -> None
        sets multiple variables in the pipeline

    get(name: str, must: bool = False) -> Any
        gets a variable from the pipeline
        If the variable is not found, it will return UNSET_VARIABLE or error if must is True

    reset()
        Clears the values of all variables

    __getitem__(name: str) -> Any
        identical to get with dict like access but must is always false and
        will return UNSET_VARIABLE if the variable is not found

    __setitem__(name: str, value: Any) -> None
        identical to set with dict like access

    """

    variables: dict[str, Any] = field(factory=dict)
    defaults: dict[str, Any] = field(default={})

    @classmethod
    def define(cls, **defaults: Any) -> "Variables":
        """
        Define a Variables object with the given default values

        Parameters
        ----------
        defaults: dict
            default values for the variables

        Returns
        -------
        a new Variables object. The returned Variables will have the default values
        provided in the `defaults` dictionary. The values of the `defaults` will be used as the default values
        for the variables when `reset` is used.

        """
        pv = cls(defaults=defaults)
        pv.sets(**defaults)
        return pv

    def set(self, name: str, value: Any) -> None:
        """
        sets a variable in the pipeline

        :param name: the name of the variable
        :param value: the value of the variable
        """
        self.variables[name] = value

    def sets(self, **kwargs: Any) -> None:
        self.variables.update(kwargs)

    def get(self, name: str, default=None, must: bool = False) -> Any:
        """
        gets a variable from the pipeline

        :param name: the name of the variable
        :param must: whether to raise an error if the variable is not found
        :returns: the value of the variable
        """
        match (
            value := self.variables.get(
                name,
                (check := (default if default is not None else self.UNSET_VARIABLE)),
            ),
            must,
        ):
            case (v, True) if v == check:
                raise UndefinedVariableException(
                    f"Variable {name} not found in pipeline variables"
                )
            case (_, _):
                return value

    def __getitem__(self, name: str) -> Any:
        return self.get(name, must=True)

    def __setitem__(self, name: str, value: Any) -> None:
        self.set(name, value)

    def __delitem__(self, name: str) -> None:
        del self.variables[name]

    def __iter__(self) -> Iterable[str]:
        return iter(self.variables)

    def __len__(self) -> int:
        return len(self.variables)

    def reset(self):
        """
        Clears the values of all variables
        """
        self.variables = self.VARIABLES_FACTORY()
        self.sets(**self.defaults)


@define
class LoggerSettings:
    """
    A data structure to hold settings for a logger.

    Pipelines do not own their logger, instead, the logger is a `ContextVar` in the `context` module.

    When Pipelines are initialized, it gets a local context that is used to run all stages. This way, stages can reference the pipeline's logger,
    as well as other relevant variables, as well as allow for deferred evaluation of stage inputs.

    Because of this approach, we cannot have a logger attribute on the pipeline. Instead, we can have a `LoggerSettings` object that is used to
    configure the logger that is stored in the local context that all stages run under.

    This allows each pipeline to function almost like its own "app" by avoiding modificaiton of global state, and allowing completely different
    logging behaviour of the same function in two different pipelines (pretty cool if you ask me)

    However, in a production environment (not that anyone would ever use my library in production), you would probably want to have a single logger
    logging json to stdout a-la 12 factor app, so just override the logger with the `override_logger` attribute.

    Attributes
    ----------
    name : str
        the name of the logger (defaults to the name of the pipeline)

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

    override_logger : Any
        A logger object to wrap in structlog. If this is provided we ignore everything else and just wrap it in structlog.

    nowrap_overriden: bool
        If this is true then the overridden logger MUST be a structlog logger already

    structlogkwds: dict[str, Any]
        A dictionary of keyword arguments to pass to structlog.wrap_logger when creating the logger. See the structlog documentation for more information.
    """

    # might replace stdstructlogger function with a class that can be used to create loggers
    name: str = field()
    stream: bool | Any = field(default=True)
    log_dir: Path | PathLike = field(factory=Path.cwd)
    file: bool | Path | PathLike | str = field(default=True)
    handlers: list[logging.Handler] | None = field(default=None)
    override_logger: Any = field(
        default=None
    )  # if this is provided we ignore everything except binds and just wrap this in structlog
    nowrap_overriden: bool = field(
        default=False
    )  # if this is true then the overridden logger MUST be a structlog logger already
    structlogkwds: dict[str, Any] = field(
        factory=dict
    )  # keyword arguments to pass to structlog.wrap_logger


@define
class Pipeline:
    """
    A Pipeline is a collection of stages that are run in sequence. It is the primary
    object for defining and running pipelines in the pypedream framework.

    Attributes
    ----------
    name : str
        the name of the pipeline

    parameters : Parameters
        a Parameters object that manages the parameters of the pipeline

    variables : Variables
        a Variables object that manages the variables of the pipeline

    stages : StageTable
        a dictionary mapping stage keys to Stage objects
    """

    name: str = field(default="Pipeline")
    parameters: Parameters = field(factory=Parameters)
    variables: Variables = field(factory=Variables)
    stages: StageTable = field(factory=dict)
    log_settings: LoggerSettings | None = Factory(
        lambda self: LoggerSettings(name=self.name), takes_self=True
    )
    ctx: contextvars.Context = field(init=False)

    def stage(
        self,
        name_or_callable: str | Callable[P, R] | None = None,
        /,
        *inputargs: StageInputs,
        inputs: StageInputs | None = None,
        output_mapper: Callable[[R], dict[str, Any]] | None = None,
        name=None,
        **defaults: P.kwargs,
    ) -> Callable[[Callable[P, R]], Callable[P, R]] | Callable[P, R]:
        """
        Registers a stage in the pipeline under the stage key `name` or under the name of the function if `name` is not provided

        Parameters
        ----------
        name_or_callable : str | Callable, optional
            the name of the stage or the function to register as a stage

        inputs: list[Input], optional

        output_mapper : Callable[[R], dict[str, Any]], optional
            a function that maps the return value of the stage to a dictionary
            of outputs. By default, this function returns a dictionary with a
            single key "return" that maps to the return value of the stage.

        defaults : dict
            default values for arguments to the stage, these will be wrapped in StageInputMapping compliant objects and
            joined with the `inputs` parameter.

        Returns
        -------
        A decorator that registers the function as a stage in the pipeline if `name_or_callable`
        is a function, otherwise returns the function itself.
        Registering a function as a stage does not modify the actual function
        so it can still be used outside the framework.
        """

        stage_key = name or None
        inputs = list(inputargs) + (inputs or [])

        def decorator(func: Callable[P, R]) -> Callable[P, R]:
            nonlocal stage_key, inputs
            if stage_key is None:
                stage_key
                stage_key = func.__name__

            if output_mapper is None:
                self.stages[stage_key] = Stage(function=func, inputs=inputs)
            else:
                self.stages[stage_key] = Stage(
                    function=func, inputs=inputs, output_mapper=output_mapper
                )

            return func

        match (stage_key is not None, name_or_callable):
            case (_, noc) if callable(noc):
                return decorator(name_or_callable)
            case (False, noc) if isinstance(noc, str):
                stage_key = noc
                return decorator
            case (True, noc) if isinstance(noc, str):
                raise ValueError(
                    "Cannot provide a name kwarg and a string as the first argument"
                )
            case (_, None):
                return decorator

        if isinstance(name_or_callable, str):
            stage_key = name_or_callable
            return decorator
        elif name_or_callable is None:
            return decorator
        elif callable(name_or_callable):
            stage_key = name_or_callable.__name__
            return decorator(name_or_callable)

    def run_stage(self, stage_key: str, **kwargs: Any) -> Any:
        """
        Runs a stage in the pipeline, using the configured inputs and outputs
        The output of the stage is returned and also stored in the stages output


        Parameters
        ----------
        stage_key : str
            the key of the stage to run

        kwargs : dict
            keyword arguments to pass to the stage, overrides any configured inputs
            not validated so be careful

        Returns
        -------
        the output of the stage
        """
        context.STAGE.set((curr := self.stages[stage_key]))
        with logging_context(stage=stage_key):
            try:
                output = curr.run(**kwargs)
            except ExitPipeline:
                raise
            except Exception as e:
                context.LOG.get().exception(e)
                raise ExitPipeline("Error in pipeline", True, e) from e
        # we dont reset the context stage so if we error we can see what stage we errored in
        return output

    def run(self, **kwargs: Any) -> dict[str, Any]:
        """
        Runs all the stages in the pipeline in sequence
        The output of each stage is stored in the stages output

        Parameters
        ----------
        kwargs : dict
            keyword arguments to pass to the stages, overrides any configured inputs
            not validated so be careful

        Returns
        -------
        a dictionary of the outputs of each stage
        """
        self.ctx.run(self._init_run_logger)
        outputs = {}
        for stage_key in self.stages:
            try:
                outputs[stage_key] = self.ctx.run(self.run_stage, stage_key, **kwargs)
            except ExitPipeline as e:
                log = self.ctx[context.LOG]
                if e.error:
                    log.exception(
                        "Exited pipeline with error state.",
                        exc_info=True,
                    )
                else:
                    log.info("Exited pipeline successfully.")
        return outputs

    @ctx.default
    def _ctx(self):
        def _applydefaults():
            nonlocal self
            context.PIPELINE.set(self)
            context.VARIABLES.set(self.variables)
            context.PARAMETERS.set(self.parameters)
            context.STAGES.set(self.stages)

        ctx = contextvars.copy_context()
        ctx.run(_applydefaults)
        return ctx

    def _init_run_logger(self):
        if self.log_settings is None:
            return
        ls = self.log_settings
        match (ls.override_logger, ls.nowrap_overriden):
            case (None, _):
                context.LOG.set(
                    stdstructlogger(
                        ls.name.replace(" ", "_"),
                        stream=ls.stream,
                        log_dir=ls.log_dir,
                        file=ls.file,
                        handlers=ls.handlers,
                    )
                )

            case (override, nowrap):
                context.LOG.set(
                    override
                    if nowrap
                    else structlog.wrap_logger(override, **ls.structlogkwds)
                )
