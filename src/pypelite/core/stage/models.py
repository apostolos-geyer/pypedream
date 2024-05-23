from typing import Callable, ParamSpec, TypeVar
from textwrap import dedent

from attrs import define, field

from pypelite.core.stage.annotation import (
    StageInputs,
    StageOutputMapper,
    StageOutputs,
)

from pypelite.core.stage.outputs import DEFAULT_OUTPUT_MAPPER

P = ParamSpec("P")
R = TypeVar("R")
T = TypeVar("T")

DEFAULT_OUTPUT_KEY = "return"
INPUT_NOT_FOUND = "INPUT NOT FOUND"


@define
class Stage:
    """
    A Stage is a wrapper around a function that is used in a Pipeline. It is currently tightly coupled to the Pipeline object.
    This may change in the future.

    Attributes
    ----------
    function : Callable[P, R]
        the function to run as the stage.

    has_run : bool
        a flag indicating whether the stage has been run in the context of a pipeline

    inputs : StageInputs
        a list of objects conforming to the StageInputMapping protocol
        that define what and how to pass arguments to the stage in the
        context of a pipeline

    outputs : StageOutputs
        a dictionary mapping keys to the results of the stage

    output_mapper : Callable[[R], StageOutputs]
        a function that maps the return value of the stage to a dictionary
        of outputs. By default, this function returns a dictionary with a
        single key "return" that maps to the return value of the stage.


    Methods
    -------
    reset()
        resets all internal state (sets has_run to False and clears outputs)
    """

    function: Callable[P, R]
    inputs: StageInputs = field(factory=StageInputs)
    has_run: bool = field(default=False)
    outputs: StageOutputs = field(factory=StageOutputs)
    output_mapper: StageOutputMapper = field(default=DEFAULT_OUTPUT_MAPPER)

    def reset(self) -> None:
        """
        resets all internal state except for defaults
        """
        self.has_run = False
        self.outputs = {}


    def __repr__(self) -> str:
        repr = dedent(
            f"""\
            Stage(
                function={self.function.__repr__()},
                inputs={self.inputs.__repr__()},
                outputs={self.outputs.__repr__()},
                output_mapper={self.output_mapper.__repr__()},
                has_run={self.has_run.__repr__()},
            )""")
        return repr
    
    def __str__(self) -> str:
        # TODO make this nicer
        return dedent(
            f"""\
            Stage(
                function={self.function.__str__()},
                inputs={self.inputs.__str__()},
                outputs={self.outputs.__str__()},
                output_mapper={self.output_mapper.__str__()},
                has_run={self.has_run.__str__()},
            )""")

