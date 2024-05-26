"""
pypedream.input contains shorthands for creating input mappings for stages
"""
from typing import Any, TypeVar

from pypedream.core.pipelines import Parameters, Variables
from pypedream.core.stages import (
    DEFAULT_OUTPUT_KEY,
    INPUT_NOT_FOUND,
    DependencyInputMapper,
    Input,
    InputBinding,
    KeyedInputMapper,
    StageTable,
)
from pypedream.ctx import PARAMETERS, STAGES, VARIABLES

T = TypeVar("T")


def known(
    value: T,
    as_arg: str,
    logged: bool = False,
) -> Input[T, T]:
    """
    Create an Input object that binds to a known value

    Parameters
    ----------
    value : T
        the known value

    as_arg : str
        the name of the argument in the function to insert the known value

    logged : bool, optional
        whether the parameter should be injected into the logging context at execution time, by default False

    Returns
    -------
    Input[T, T]
        an Input object
    """
    return Input(
        as_arg=as_arg,
        bind=InputBinding.now(value),
        logged=logged,
    )


def param(
    name: str,
    as_arg: str,
    default: Any = INPUT_NOT_FOUND,
    required: bool = True,
    logged: bool = False,
) -> Input[Parameters, Any]:
    """
    Create an Input object that binds to a parameter

    Parameters
    ----------
    name : str
        the name of the parameter

    as_arg : str
        the name of the argument in the function to insert the parameter value

    default : Any, optional
        the default value of the parameter, by default `INPUT_NOT_FOUND`

    required : bool, optional
        whether the parameter is required, by default True

    logged : bool, optional
        whether the parameter should be injected into the logging context at execution time, by default False

    Returns
    -------
    Input[Parameters, Any]
        an Input object
    """
    return Input(
        as_arg=as_arg,
        bind=InputBinding.deferred(
            PARAMETERS,
            mapper=(
                KeyedInputMapper(
                    from_key=name,
                    default=default,
                    required=required,
                )
            ),
        ),
        logged=logged,
    )


def var(
    name: str,
    as_arg: str,
    default: Any = INPUT_NOT_FOUND,
    required: bool = True,
    logged: bool = False,
) -> Input[Variables, Any]:
    """
    Create an Input object that binds to a variable

    Parameters
    ----------
    name : str
        the name of the variable

    as_arg : str
        the name of the argument in the function to insert the variable value

    default : Any, optional
        the default value of the variable, by default `INPUT_NOT_FOUND`

    required : bool, optional
        whether the variable is required, by default True

    logged : bool, optional
        whether the parameter should be injected into the logging context at execution time, by default False

    Returns
    -------
    Input[Variables, Any]
        an Input object
    """
    return Input(
        as_arg=as_arg,
        bind=InputBinding.deferred(
            VARIABLES,
            mapper=(
                KeyedInputMapper(
                    from_key=name,
                    default=default,
                    required=required,
                )
                if default is not None
                else KeyedInputMapper(from_key=name, required=required)
            ),
        ),
        logged=logged,
    )


# how you would define a dependency on a stage without shorthands:
# Input(as_arg="new_data", bind=InputBinding.deferred(ctx.STAGES, mapper=DependencyInputMapper(from_stage="combine_data")))
# we can do better


def dependency(
    from_stage: str,
    as_arg: str,
    from_output: str = DEFAULT_OUTPUT_KEY,
    default: Any | None = None,
    required: bool = True,
    logged: bool = False,
) -> Input[StageTable, Any]:
    """
    Create an input object that binds to an output of another stage

    Parameters
    ----------
    from_stage : str
        the name of the stage depended on

    as_arg : str
        the name of the argument in the function to insert the variable value

    from_output : str
        the output key that we want from the stage, by default `DEFAULT_OUTPUT_KEY`

    default : Any, optional
        the default value of the variable, by default `INPUT_NOT_FOUND`

    required : bool, optional
        whether the variable is required, by default True

    logged : bool, optional
        whether the parameter should be injected into the logging context at execution time, by default False
    """

    return Input(
        as_arg=as_arg,
        bind=InputBinding.deferred(
            STAGES,
            DependencyInputMapper(
                from_stage=from_stage,
                from_output=from_output,
                default=default,
                required=required,
            ),
        ),
        logged=logged,
    )
