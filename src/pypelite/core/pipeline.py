import logging
from typing import Callable, ParamSpec, TypeVar, overload, Any, Iterable

from attrs import define, field

from pypelite.core.stage import Stage
from pypelite.core.stage.inputs import (
    DefaultInput,
    DependencyInput,
    KeyedInput,
    bind_inputs,
)

from pypelite.core.stage.annotation import (
    StageTable,
    StageInputs,
    StageOutputMapper,
)

__all__ = [
    "Pipeline",
    "Parameters",
    "Variables",
]

P = ParamSpec("P")
R = TypeVar("R")


@define
class Parameters:
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
    def define(cls, parameter_set: Iterable[str], **defaults: Any) -> "Parameters":
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
            raise ValueError(
                f"Attempting to set parameter {name} not found in pipeline parameters"
            )

        self.parameters[name] = value

    def sets(self, **kwargs: Any) -> None:
        """
        sets multiple parameters in the pipeline

        :param kwargs: a dictionary of parameters and values
        """
        if any(name not in self.parameter_set for name in kwargs):
            raise ValueError(
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
            raise ValueError(
                f"Attempting to retrieve undeclared parameter {name} from pipeline parameters"
            )
        match (
            value := self.parameters.get(
                name, (check := default or self.UNSET_PARAMETER)
            ),
            must,
        ):
            case (v, True) if v == check:
                raise ValueError(
                    f"Declared but unset paramater {name} not found in pipeline parameters"
                )
            case (_, _):
                return value

    def __getitem__(self, name: str) -> Any:
        return self.get(name)

    def __setitem__(self, name: str, value: Any) -> None:
        self.set(name, value)

    def __contains__(self, name: str) -> bool:
        return name in self.parameters

    def reset(self):
        """
        Clears the values of all parameters, does not modify the parameter set.
        """
        self.parameters = self.PARAMATERS_FACTORY()
        self.sets(**self.defaults)


@define
class Variables:
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
                name, (check := (default or self.UNSET_VARIABLE))
            ),
            must,
        ):
            case (v, True) if v == check:
                raise ValueError(f"Variable {name} not found in pipeline variables")
            case (_, _):
                return value

    def __getitem__(self, name: str) -> Any:
        return self.get(name)

    def __setitem__(self, name: str, value: Any) -> None:
        self.set(name, value)

    def __contains__(self, name: str) -> bool:
        return name in self.variables

    def reset(self):
        """
        Clears the values of all variables
        """
        self.variables = self.VARIABLES_FACTORY()
        self.sets(**self.defaults)


@define
class Pipeline:
    """
    A Pipeline is a collection of stages that are run in sequence. It is the primary
    object for defining and running pipelines in the pypelite framework.

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

    logger : logging.Logger
        a logger for the pipeline

    Methods
    -------
    stage(*args, **kwargs) -> Callable
        see the stage method for more information, registers a stage in the pipeline

    """

    name: str = field(default="Pipeline")
    parameters: Parameters = field(factory=Parameters)
    variables: Variables = field(factory=Variables)
    stages: StageTable = field(default={})
    logger: logging.Logger = field(init=False)

    @overload
    def stage(
        self,
        name: str | None = None,
        *,
        use_defaults: list[DefaultInput] | None = None,
        use_stages: list[DependencyInput] | None = None,
        use_variables: list[KeyedInput] | None = None,
        use_parameters: list[KeyedInput] | None = None,
        output_mapper: StageOutputMapper | None = None,
        **defaults: P.kwargs,
    ) -> Callable[[Callable[P, R]], Callable[P, R]]:
        """
        Registers a stage in the pipeline under the stage key `name` or under the name of the function if `name` is not provided

        Parameters
        ----------
        name : str, optional
            the name of the stage

        use_defaults: list[DefaultInput], optional
            the default values to use from the pipeline,

        use_stages: list[DependencyInput], optional
            the outputs to use from the pipeline,

        use_variables: list[KeyedInput], optional
            the variables to use from the pipeline,

        use_parameters: list[KeyedInput], optional
            the parameters to use from the pipeline,

        output_mapper : StageOutputMapper, optional
            a function that maps the return value of the stage to a dictionary
            of outputs. By default, this function returns a dictionary with a
            single key "return" that maps to the return value of the stage.

        defaults : dict
            default values for arguments to the stage, these will be wrapped in StageInputMapping compliant objects and
            joined with the `use_defaults` parameter.

        Returns
        -------
        A decorator that registers the function as a stage in the pipeline.
        """
        ...

    @overload
    def stage(
        self,
        func: Callable[P, R],
    ) -> Callable[P, R]:
        """
        Registers a stage in the pipeline under the name of the function

        Parameters
        ----------
        func : Callable
            the function to register as a stage

        Returns
        -------
        the function
        """
        ...

    def stage(
        self,
        name_or_callable: str | Callable[P, R] | None = None,
        *,
        use_defaults: list[DefaultInput] | None = None,
        use_stages: list[DependencyInput] | None = None,
        use_variables: list[KeyedInput] | None = None,
        use_parameters: list[KeyedInput] | None = None,
        output_mapper: StageOutputMapper | None = None,
        **defaults: P.kwargs,
    ) -> Callable[[Callable[P, R]], Callable[P, R]] | Callable[P, R]:
        """
        Registers a stage in the pipeline under the stage key `name` or under the name of the function if `name` is not provided

        Parameters
        ----------
        name_or_callable : str | Callable, optional
            the name of the stage or the function to register as a stage

        use_defaults: list[DefaultInput], optional
            the default values to use from the pipeline,

        use_stages: list[DependencyInput], optional
            the outputs to use from the pipeline,

        use_variables: list[KeyedInput], optional
            the variables to use from the pipeline,

        use_parameters: list[KeyedInput], optional
            the parameters to use from the pipeline,

        output_mapper : StageOutputMapper, optional
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

        stage_key = None
        use_defaults = (use_defaults or []) + [
            DefaultInput(as_arg=key, value=defaults[key]) for key in defaults
        ]
        use_stages = bind_inputs((use_stages or []), self.stages)
        use_variables = bind_inputs((use_variables or []), self.variables)
        use_parameters = bind_inputs((use_parameters or []), self.parameters)
        inputs: StageInputs = use_defaults + use_stages + use_variables + use_parameters

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

        if isinstance(name_or_callable, str):
            stage_key = name_or_callable
            return decorator
        elif name_or_callable is None:
            return decorator
        elif callable(name_or_callable):
            stage_key = name_or_callable.__name__
            return decorator(name_or_callable)
