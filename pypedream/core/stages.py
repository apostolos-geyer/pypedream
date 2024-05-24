from contextvars import ContextVar
from typing import Callable, Generic, Literal, ParamSpec, TypeVar, Any
from textwrap import dedent

from attrs import define, field

from enum import Flag, auto
from functools import cached_property
from typing import Mapping, Sequence

from attrs import frozen


P = ParamSpec("P")
R = TypeVar("R")
T = TypeVar("T")

StageKey = str
StageInputs = list["Input"]
StageOutputs = dict[str, Any]
StageTable = dict[StageKey, "Stage"]


__all__ = [
    # Main stage type and related aliases
    "Stage",
    "StageKey",
    "StageInputs",
    "StageOutputs",
    "StageTable",
    # Output related enums, functions, constants, etc
    "DEFAULT_OUTPUT_KEY",
    "OutputMapperBehaviour",
    "STRICT",
    "CHILL",
    "PRESERVE",
    "WARN_UNEXPECTED",
    "DEFAULT_OUTPUT_MAPPER",
    "SequentialOutputMapper",
    "KeyedOutputMapper",
    # Input related enums, functions, constants, etc
    "INPUT_NOT_FOUND",
    "prepare_inputs",
    "must_bind",
    "Input",
    "InputBinding",
    "UnboundInputException",
    "UndefinedInputException",
    "DependencyInputMapper",
    "KeyedInputMapper",
]


# ------------------------------
# OUTPUT MAPPERS
# ------------------------------
DEFAULT_OUTPUT_KEY = "return"


class OutputMapperBehaviour(Flag):
    """
    Enum defining some basic behaviours for output mapper objects, mostly pertaining to how to handle
    outputs that are not explicitly defined in the mapper.
    """

    STRICT = auto()
    " Raise an error if the mapper receives an input that does not have an explicitly defined mapping. "

    CHILL = auto()
    " Map what can be mapped, ignore the rest (extra or missing).. just chill out. "

    PRESERVE = auto()
    """
    Map what can be mapped explicitly as expected, and handle mapping of unexpected outputs in a way that preserves them.
    The key that unexpected outputs will be stored under is implementation specific.
    """

    WARN_UNEXPECTED = auto()
    " Log a warning on unexpected outputs. This should be used in conjunction with another behaviour. "

    @staticmethod
    def _attrsvalidator(self, _, value):
        """
        A validator for the OutputMapperBehaviour enum that ensures that the value makes logical sense, for attrs class use.
        """
        if not isinstance(value, OutputMapperBehaviour):
            raise ValueError(
                "Invalid behaviour value. Should be an OutputMapperBehaviour enum value or union of them"
            )

        if not any(behaviour in value for behaviour in (STRICT, CHILL, PRESERVE)):
            raise ValueError(
                "Invalid behaviour value. Should include STRICT, CHILL, or PRESERVE to determine how to handle unexpected outputs."
            )


STRICT, CHILL, PRESERVE, WARN_UNEXPECTED = OutputMapperBehaviour


def DEFAULT_OUTPUT_MAPPER(output: Any) -> StageOutputs:
    """
    Default output mapper that maps the return value of a stage to a dictionary
    with a single key `DEFAULT_OUTPUT_KEY`

    Parameters
    ----------
    x : Any
        the return value of the stage

    Returns
    -------
    A dictionary with the value of x mapped to the key `DEFAULT_OUTPUT_KEY`
    """
    return {DEFAULT_OUTPUT_KEY: output}


@frozen
class SequentialOutputMapper:
    """
    An implementation of the StageOutputMapper protocol that maps an indexable sequence of values to a dictionary of outputs.

    Attributes
    ----------
    keys : list[str]
        a list of keys to map the values to. The length of this list should match the length of the sequence.
        If the length of the sequence is not equal to the length of the keys, the behaviour will determine how to handle this.

    behaviour : OutputMapperBehaviour
        flag indicating how to handle unexpected outputs in the sequence.
        - `STRICT`: raise an error if the length of the iterable does not match the length of the keys
        - `CHILL`: map what can be mapped, ignore the rest (extra or missing).. just chill out.
        - `PRESERVE`: Extra outputs will be stored under `SequenceOutputMapper.EXTRA_MAPPING_KEY`
        - `WARN_UNEXPECTED`: log a warning on unexpected outputs. This should be used in conjunction with another behaviour.

    Methods
    -------
    __call__(x: Sequence[Any]) -> StageOutputs
        Maps the iterable x to a dictionary of outputs.
    """

    EXTRA_MAPPING_KEY = "_extra"
    keys: list[str] = field(factory=list)
    behaviour: OutputMapperBehaviour = field(
        default=STRICT, validator=OutputMapperBehaviour._attrsvalidator
    )

    def _handle_unexpected(self, x: Sequence[Any]) -> StageOutputs:
        """
        Handle unexpected outputs based on the behaviour.

        Parameters
        ----------
        x : Sequence[Any]
            the sequence to map to outputs

        Returns
        -------
        A dictionary with the values of x mapped to the keys in `self.keys`

        Raises
        ------
        ValueError
            if self.behaviour is STRICT
        """

        seqlen = len(x)
        keylen = len(self.keys)
        longer = seqlen > keylen

        behaviours = list(self.behaviour)

        match behaviours:
            case [OutputMapperBehaviour.STRICT]:
                raise ValueError(
                    f"Length of x ({len(x)}) does not match length of keys ({len(self.keys)})"
                )

            case [OutputMapperBehaviour.CHILL, *etc]:
                mapping = (
                    {k: v for k, v in zip(self.keys, x[0:keylen])}
                    if longer
                    else {k: v for k, v in zip(self.keys[0:seqlen], x)}
                )
                if OutputMapperBehaviour.WARN_UNEXPECTED in etc:
                    # TODO: log a warning, with more info
                    print(
                        "Some outputs were not mapped. TODO: MAKE THIS LOG A WARNING AND BE MORE INFORMATIVE"
                    )
                return mapping

            case [OutputMapperBehaviour.PRESERVE, *etc]:
                mapping = (
                    {k: v for k, v in zip(self.keys, x[0:keylen])}
                    if longer
                    else {k: v for k, v in zip(self.keys[0:seqlen], x)}
                )
                if longer:
                    extra = x[keylen:]
                    mapping.update({self.EXTRA_MAPPING_KEY: extra})

                if OutputMapperBehaviour.WARN_UNEXPECTED in etc:
                    # TODO: log a warning, with more info
                    print(
                        "Some outputs were not mapped. TODO: MAKE THIS LOG A WARNING AND BE MORE INFORMATIVE"
                    )
                return mapping

            case _:
                raise ValueError(
                    "Invalid behaviour value. Should include STRICT, CHILL, or PRESERVE to determine how to handle unexpected outputs."
                )

    def __call__(self, x: Sequence[Any]) -> StageOutputs:
        """
        Maps the iterable x to a dictionary of outputs.

        Parameters
        ----------
        x : Sequence[Any]
            the sequence to map to outputs

        Returns
        -------
        A dictionary with the values of x mapped to the keys in `self.keys`

        Raises
        ------
        ValueError
            if the length of x does not match the length of `self.keys` and `self.strict` is True
        """
        return (
            {k: v for k, v in zip(self.keys, x)}
            if len(x) == len(self.keys)
            else self._handle_unexpected(x)
        )

    def __repr__(self) -> str:
        return f"SequentialOutputMapper(keys={self.keys})"

    @cached_property
    def __str__(self) -> str:
        string = "SequentialOutputMapper(\n"
        for i, key in enumerate(self.keys):
            string += f" Ouput[{i}] -> {key}\n"
        string += ")"
        return string


@frozen
class KeyedOutputMapper:
    """
    An implementation of the StageOutputMapper protocol that maps a dictionary of values to a dictionary of outputs.

    Attributes
    ----------
    keys : dict[str, str]
        a dictionary mapping keys to look for in output to the keys to store values under.
        If a key in the dictionary is not in the keys, an error will be raised.

    behaviour : OutputMapperBehaviour
        a flag indicating how to handle unexpected outputs in the input dictionary.
        - `STRICT`: raise an error if a key in the input dictionary is not in `self.keys`
        - `CHILL`: map what can be mapped, ignore the rest (extra or missing).. just chill out.
        - `PRESERVE`: Extra outputs will be stored under the key that they were found under in the input dictionary.
            User should be careful to avoid key conflicts.
        - `WARN_UNEXPECTED`: log a warning on unexpected outputs. This should be used in conjunction with another behaviour.
    """

    keys: dict[str, str] = field(factory=dict)
    behaviour: OutputMapperBehaviour = field(
        default=STRICT, validator=OutputMapperBehaviour._attrsvalidator
    )

    def _handle_unexpected(self, x: dict[str, Any]) -> StageOutputs:
        """
        Handle unexpected outputs based on the behaviour.

        Parameters
        ----------
        x : dict[str, Any]
            the dictionary to map to outputs

        Returns
        -------
        A dictionary with the values of x mapped to the keys in `self.keys`

        Raises
        ------
        ValueError
            if self.behaviour is STRICT
        """

        behaviours = list(self.behaviour)

        match behaviours:
            case [OutputMapperBehaviour.STRICT]:
                raise ValueError(
                    f"Keys in x ({x.keys()}) do not match keys in self.keys ({self.keys.keys()})"
                )

            case [OutputMapperBehaviour.CHILL, *etc]:
                mapping = {self.keys[k]: x[k] for k in self.keys if k in x}
                if WARN_UNEXPECTED in etc:
                    print(
                        "Some outputs were not mapped. TODO: MAKE THIS LOG A WARNING AND BE MORE INFORMATIVE"
                    )
                return mapping

            case [OutputMapperBehaviour.PRESERVE, *etc]:
                mapping = {self.keys[k]: x[k] for k in self.keys if k in x}
                extra = {k: v for k, v in x.items() if k not in self.keys}
                mapping.update(extra)
                if WARN_UNEXPECTED in etc:
                    print(
                        "Some outputs were not mapped. TODO: MAKE THIS LOG A WARNING AND BE MORE INFORMATIVE"
                    )
                return mapping

            case _:
                raise ValueError(
                    "Invalid behaviour value. Should include STRICT, CHILL, or PRESERVE to determine how to handle unexpected outputs."
                )

    def __call__(self, x: Mapping[str, Any]) -> StageOutputs:
        """
        Maps the dictionary x to a dictionary of outputs.

        Parameters
        ----------
        x : dict[str, Any]
            the dictionary to map to outputs

        Returns
        -------
        A dictionary with the values of x mapped to the keys in `self.keys`

        Raises
        ------
        ValueError
            if a key in x is not in `self.keys`
        """
        all_expected = set(self.keys.keys()) == set(x.keys())

        return (
            {self.keys[k]: x[k] for k in self.keys}
            if all_expected
            else self._handle_unexpected(x)
        )

    def __repr__(self) -> str:
        return f"MappingOutputMapper(keys={self.keys})"

    @cached_property
    def __str__(self) -> str:
        string = "MappingOutputMapper(\n"
        for key, value in self.keys.items():
            string += f" {key} -> {value}\n"
        string += ")"
        return string


__all__ = [
    "INPUT_NOT_FOUND",
    "prepare_inputs",
]

INPUT_NOT_FOUND = "INPUT NOT FOUND"


BS = TypeVar("BS")
BV = TypeVar("BV")
T = TypeVar("T")
R = TypeVar("R")


UNBOUND = "UNBOUND"
UNBOUND_T = Literal["UNBOUND"]


# ------------------------------
# INPUT TYPE
# ------------------------------
@define
class Input(Generic[T, R]):
    """
    The Input class is a generic class that represents an input to a stage.

    Attributes
    ----------
    as_arg : str
        the argument to pass the input as. This should match the name of the argument in the function signature of
        the stage that the input is being passed to.

    binding : Binding[T, R]
        the binding that retrieves the input value when called.


    Methods
    -------
    get() -> dict[str, R]
        gets the input value from the binding and returns it as a dictionary with the key `as_arg`.
    """

    as_arg: str
    bind: "InputBinding[T, R]"

    def get(self) -> dict[str, R]:
        return {self.as_arg: self.bind()}


class UnboundInputException(Exception):
    pass


class UndefinedInputException(Exception):
    pass


# ------------------------------
# INPUT BINDING TYPE
# ------------------------------
# if this works out im totally getting rid of all the other input types and having
# one input type and different binding factories.. which i think is so much cleaner
# and more flexible... dependency inversion


def must_bind(x: T) -> T:
    """
    A function that raises an UnboundInputException if the input is unbound. Used as a default mapper for bindings.
    """
    if x == UNBOUND:
        raise UnboundInputException
    return x


@define
class InputBinding(Generic[BS, BV]):
    source: BS | UNBOUND_T = field(default=UNBOUND)
    mapper: Callable[[BS], BV] | Callable[[BS | UNBOUND_T], BV] = field(
        default=must_bind
    )
    defer: ContextVar[BS] | None = field(default=None)

    def __call__(self) -> BV:
        # we never force an unbound input to raise an exception, the mapper decides what to do
        match (self.source, self.defer):
            case ("UNBOUND", _):
                return self.mapper(self.defer.get())
            case (_, _):
                return self.mapper(self.source)

    @classmethod
    def now(
        cls,
        source: T,
        mapper: Callable[[T], R] = must_bind,
    ) -> "InputBinding[T, R]":
        return cls(source=source, mapper=mapper)

    @classmethod
    def deferred(
        cls,
        source: ContextVar[T],
        mapper: Callable[[T], R] = must_bind,
    ) -> "InputBinding[T, R]":
        return cls(source=UNBOUND, mapper=mapper, defer=source)


# ------------------------------
# INPUT MAPPERS
# ------------------------------
# now we re define all the dependency input, keyed input, and default input as functions that return
# other functions to be passed into the Binding class. This is a much more flexible and clean way to
# define inputs and their bindings.


@define
class DependencyInputMapper(Generic[T, R]):
    from_stage: StageKey = field()
    from_output: str = field(default=DEFAULT_OUTPUT_KEY)
    default: T = field(default=INPUT_NOT_FOUND)
    required: bool = field(default=True)

    def __call__(self, source: StageTable | UNBOUND_T) -> T:
        if (source is UNBOUND) and self.required:
            raise UnboundInputException
        elif ((stage := source.get(self.from_stage, None)) is None) and self.required:
            raise UndefinedInputException(
                f"Stage {self.from_stage} not found in source, hence output {self.from_output} not found."
            )
        elif not stage:
            return self.default
        else:
            match (stage.has_run, self.from_output in stage.outputs, self.required):
                case (False, _, True):
                    raise UndefinedInputException(
                        f"Stage {self.from_stage} has not run yet. Cannot get output {self.from_output}."
                    )
                case (True, False, True):
                    raise UndefinedInputException(
                        f"Stage {self.from_stage} completed and did not produce output {self.from_output}."
                    )
                case (True, _, _):
                    return stage.outputs.get(self.from_output, self.default)
                case _:
                    raise Exception("what the f")


@define
class KeyedInputMapper(Generic[T, R]):
    from_key: str = field()
    default: T = field(default=INPUT_NOT_FOUND)
    required: bool = field(default=True)

    def __call__(self, source: Mapping[str, Any] | UNBOUND_T) -> T:
        match (source is UNBOUND, self.required):
            case (True, True):
                raise UnboundInputException
            case (True, False):
                return self.default
            case (False, True):
                if self.from_key not in source:
                    raise UndefinedInputException(
                        f"Key {self.from_key} not found in source."
                    )
                return source[self.from_key]
            case (False, False):
                return source.get(self.from_key, self.default)


# no need for default input because we can use the value directly in binding.now
# god this is SOOOO much cleaner why do i keep footgunning myself with these classes


def prepare_inputs(inputs: StageInputs) -> dict[str, Any]:
    """
    Prepare inputs to be passed in to a stage. All inputs must be
    bound to a source before calling this method.

    Parameters
    ----------
    inputs : list[StageInputMapping]
        the list of input mappings to prepare

    Returns
    -------
    a dictionary of inputs
    """
    prepared_inputs = {}
    for input_mapping in inputs:
        prepared_inputs.update(input_mapping.get())

    return prepared_inputs


@define
class Stage(Generic[P, R]):
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
    run(**kwargs) -> R
        runs the stage with the given keyword arguments, which will override any inputs.
        Returns the return value of the function.

    reset()
        resets all internal state (sets has_run to False and clears outputs)
    """

    function: Callable[P, R]
    inputs: StageInputs = field(factory=StageInputs)
    has_run: bool = field(default=False)
    outputs: StageOutputs = field(factory=StageOutputs)
    output_mapper: Callable[[R], StageOutputs] = field(default=DEFAULT_OUTPUT_MAPPER)

    def run(self, **kwargs) -> R:
        """
        runs the stage, kwargs will override any inputs.

        Parameters
        ----------
        kwargs : dict
            keyword arguments that will override the inputs of the stage, be careful as these are not
            validated so you may get unexpected results if you pass the wrong keys and the function
            signature does not accept arbitrary keyword arguments.

        Returns
        -------
        R
            the return value of the function, which is also stored in the outputs attribute,
            be careful if this is something mutable.

        """
        inputs = prepare_inputs(self.inputs, kwargs)
        inputs.update(kwargs)
        output = self.function(**inputs)
        self.outputs = self.output_mapper(output)
        self.has_run = True
        return output

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
            )"""
        )
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
            )"""
        )
