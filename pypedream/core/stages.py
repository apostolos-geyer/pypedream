from contextvars import ContextVar
from warnings import warn
from enum import Flag, auto
from functools import cached_property
from textwrap import dedent
from typing import (
    Any,
    Callable,
    Generic,
    Literal,
    Mapping,
    ParamSpec,
    Sequence,
    TypeAlias,
    TypeVar,
)

from attrs import define, field, frozen

from pypedream.core.logs import logging_context

P = ParamSpec("P")
R = TypeVar("R")
T = TypeVar("T")

StageKey: TypeAlias = str
StageInputs: TypeAlias = list["Input"]
StageOutputs: TypeAlias = dict[str, Any]
StageTable: TypeAlias = dict[StageKey, "Stage"]


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
    "_prepare_inputs_and_context",
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
        if not isinstance(value, OutputMapperBehaviour):  # pragma: no cover
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
    A callable used with stage that maps an indexable sequence of values to a dictionary of outputs.

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
                    warn(
                        f"Length of outputs received did not match expected. want: {keylen}, have: {seqlen}"
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
                    warn(
                        f"Length of outputs received did not match expected. want: {keylen}, have: {seqlen}"
                    )
                return mapping

            case _:  # pragma: no cover
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

    def __repr__(self) -> str:  # pragma: no cover
        return f"SequentialOutputMapper(keys={self.keys})"

    def __str__(self) -> str:  # pragma: no cover
        string = "SequentialOutputMapper(\n"
        for i, key in enumerate(self.keys):
            string += f" Ouput[{i}] -> {key}\n"
        string += ")"
        return string


@frozen
class KeyedOutputMapper:
    """
    A callable used with stage that maps dictionary return values to a dictionary of outputs.

    Attributes
    ----------
    keys : dict[str, str]
        a dictionary mapping keys to look for in output to the keys to store values under.
        If a key in the dictionary is not in the keys, an error will be raised.

    behaviour : OutputMapperBehaviour
        a flag indicating how to handle unexpected outputs in the input dictionary.
        - `STRICT`: raise an error if a key in the input dictionary is not in `self.keys`
        - `CHILL`: map what can be mapped, ignore the rest (extra or missing).. just chill out.
        - `PRESERVE`: Extra outputs will be stored under the key that they were found under in the input dictionary. User should be careful to avoid key conflicts.
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
                    warn(
                        f"Keys of outputs received did not match expected. want: {set(self.keys.keys())}, have: {set(x.keys())}"
                    )
                return mapping

            case [OutputMapperBehaviour.PRESERVE, *etc]:
                mapping = {self.keys[k]: x[k] for k in self.keys if k in x}
                extra = {k: v for k, v in x.items() if k not in self.keys}
                mapping.update(extra)
                if WARN_UNEXPECTED in etc:
                    warn(
                        f"Keys of outputs received did not match expected. want: {set(self.keys.keys())}, have: {set(x.keys())}"
                    )
                return mapping

            case _:  # pragma: no cover
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

    def __repr__(self) -> str:  # pragma: no cover
        return f"MappingOutputMapper(keys={self.keys})"

    def __str__(self) -> str:  # pragma: no cover
        string = "MappingOutputMapper(\n"
        for key, value in self.keys.items():
            string += f" {key} -> {value}\n"
        string += ")"
        return string


INPUT_NOT_FOUND = "INPUT NOT FOUND"


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

    bind : InputBinding[T, R]
        the binding that retrieves the input value when called.

    logged : bool
        a flag indicating whether or not the input should be injected into the logging context when the stage is run.

    """

    as_arg: str = field()
    bind: "InputBinding[T, R]" = field()
    logged: bool = field(default=False)

    def get(self) -> dict[str, R]:
        """
        Returns
        -------
        the input value as a dictionary with the key `as_arg`
        """
        return {self.as_arg: self.bind()}


# ------------------------------
# INPUT BINDING TYPE
# ------------------------------
# if this works out im totally getting rid of all the other input types and having
# one input type and different binding factories.. which i think is so much cleaner
# and more flexible... dependency inversion


def must_bind(x: T | UNBOUND_T) -> T:
    """
    A function that raises a ValueError if the input is unbound. Used as a default mapper for bindings.
    """
    if x == UNBOUND:
        raise ValueError("Unbound input")
    return x


BindingMapperType = (
    Callable[[T], R]
    | Callable[[T | UNBOUND_T], R]
    | Callable[[ContextVar[T]], R]
    | Callable[[Callable[..., T]], R]
)


def _unwrap_ctxvar_then_apply(
    mapper: Callable[[T | UNBOUND_T], R] | Callable[[T], R],
) -> Callable[[ContextVar[T]], R]:
    def wrapper(var: ContextVar[T]) -> R:
        return mapper(var.get())

    return wrapper


def _call_first_then_apply(
    mapper: Callable[[T | UNBOUND_T], R] | Callable[[T], R],
    *args: P.args,
    **kwargs: P.kwargs,
) -> Callable[[Callable[P, T]], R]:
    """
    returns a closure that accepts a function `fun` and applies `mapper` to `fun(*args, **kwargs)`

    Parameters
    ----------
    *args: P.args
        positional arguments to pass to the function passed to the closure

    **kwargs: P.kwargs
        keyword arguments to pass to the function passed to the closure

    Returns
    -------
    Callable[[Callable[P, T]], R]
        function that takes in another function with signature f: **P -> T, and applies a mapper g:T->R
    """

    def wrapper(fun: Callable[P, T]) -> R:
        return mapper(fun(*args, **kwargs))

    return wrapper


@define
class InputBinding(Generic[T, R]):
    """
    An `InputBinding[T, R]` is used as part of an `Input` to a `Stage`.

    i.e, suppose `Stage` has an argument of type `R`. To define an input to it we would
    use a binding to a source of type `T` with a mapper `f:T->R` that retrieves the
    value for the argument at the time of execution.

    It can either be a binding to a value known at the time of instantiation (of the binding)
    in which case you would use `InputBinding.immediate`, or a value to be retrieved
    from a context variable at execution time (of the `Stage` taking the `Input` that owns
    the `InputBinding`) in which case you'd use `InputBinding.contextual`, or to a function to
    be called at evaluation time via `InputBinding.callback`

    See the docs of the aformentioned methods for more information.

    Attributes
    ----------
    source : T | UNBOUND_T, optional
        The `source` of the input, this may be the value itself (i.e T == R) or a value
        from which the input can be retrieved. How it is retrieved from the source is handled
        by the `mapper`. An uninitialized InputBinding or one that is evaluated at execution
        time via a `ContextVar` will have `source == UNBOUND`.

    mapper : BindingMapperType, optional
        The function called when the `InputBinding` is evaluated. The `mapper` is responsible
        for behaviours like default values, error handling, etc.

    defer : ContextVar[T] | Callable[..., T] | None, optional
        The `ContextVar` to defer to, if any. If
    """

    source: T | UNBOUND_T = field(default=UNBOUND)
    mapper: BindingMapperType[T, R] = field(default=must_bind)
    defer: ContextVar[T] | Callable[..., T] | None = field(default=None)

    def __call__(self: "InputBinding[T, R]") -> R:
        """
        Get the value of the input.

        Behaviour is as follows:
        let S = `self.source` is not `UNBOUND`, D = `self.defer` is not `None`

        S or not D -> apply `self.mapper` to `self.source`
        - a defined source takes precedence even if defer is defined
        - mapper is expected to handle the possibility of source being undefined

        not S and D -> apply `self.mapper` to `self.defer`
        - if the `InputBinding` instance was defined with `InputBinding.deferred(..., get_first=True)` then
            `self.defer.get()` will be called first and passed into `self.mapper` and a possible `LookupError`
            will not be handled.
        - Otherwise the `mapper` must manually unwrap the `ContextVar`

        Returns
        -------
        R
        The result of `self.mapper(self.source)` or `self.mapper(self.defer)` as defined above
        """
        # we never force an unbound input to raise an exception, the mapper decides what to do
        # however it is the default behaviour.
        try:
            match (self.source is not UNBOUND, self.defer is not None):
                case (True, _) | (_, False):
                    return self.mapper(self.source)
                case _:
                    return self.mapper(self.defer)
        except Exception as e:
            raise UnboundInputException(self) from e

    @classmethod
    def immediate(
        cls,
        source: T,
        mapper: Callable[[T], R] = must_bind,
    ) -> "InputBinding[T, R]":
        """
        Bind to a known value that can be directly passed in to the input or mapped from

        Parameters
        ----------
        source : T
            The source of the input

        mapper : Callable[[T], R]
            A callable to apply to `source` to get the input value. `must_bind` by default, which will
            just return `source` given it is not `UNBOUND`.

        Returns
        -------
        InputBinding[T, R]
        """
        return cls(source=source, mapper=mapper)

    @classmethod
    def contextual(
        cls,
        source: ContextVar[T],
        mapper: BindingMapperType[T, R] = must_bind,
        get_first: bool = True,
    ) -> "InputBinding[T, R]":
        """
        An input binding that defers the evaluation of the input until the stage is run
        via a ContextVar

        Parameters
        ----------
        source : ContextVar[T]
            A `ContextVar` referencing the source of the value, to be retrieved from in the
            execution context of the stage

        mapper : BindingMapperType[T, R], optional
            A function that expects either a `T`, the unit type `UNBOUND_T` (value `UNBOUND`), or
            a `ContextVar[T]`. The correct signature depends on the next argument `get_first`.
            Defaults to `must_bind`

        get_first : bool, optional
            Whether or not to call `.get()` on the `ContextVar` being deferred to before passing it
            into the `mapper`. Defaults to `True` so it can be passed into `must_bind` which will
            just return the value. However, this means that if the `.get()` raises `LookupError` it
            will not be handled.
            To handle the case where `.get()` fails, pass `get_first = False` and a custom `mapper`.

        Returns
        -------
        InputBinding[T, R]
        """

        return cls(
            mapper=mapper if not get_first else _unwrap_ctxvar_then_apply(mapper),
            defer=source,
        )

    @classmethod
    def callback(
        cls,
        fun: Callable[P, T],
        *cbargs: P.args,
        mapper: BindingMapperType[T, R] = must_bind,
        call_first: bool = True,
        **cbkwargs: P.kwargs,
    ) -> "InputBinding[T, R]":
        """
        An InputBinding that defers the evaluation of the input until the stage is run via
        a closure.

        Parameters
        ----------
        fun : Callable[P, T]
            A function taking paramaters P returning a value of type T to be called at
            evaluation of the input

        *cbargs : P.args
            Positional arguments matching the signature of `fun`. Passed to `fun` at evaluation time.

        mapper : BindingMapperType[T, R], optional
            Function that will take the output of `fun` and map it to the expected type of the input,
            default `must_bind` as usual

        call_first : bool, optional
            whether or not `fun` should be called and have the result passed into `mapper`
            or just be directly passed in to `mapper`. if `False` then `*cbargs` and `**cbkwargs` are
            meaningless as the class has no power to enforce how the function is called, and
            you should be using a custom mapper... If you're actually meaning to use a function
            as an argument to a stage then bind with `InputBinding.immediate`

        **cbkwargs : P.kwargs
            Keyword arguments matching the signature of `fun`. Passed to `fun` at evaluation time.

        Returns
        -------
        InputBinding[T, R]
        """

        return cls(
            mapper=mapper
            if not call_first
            else _call_first_then_apply(mapper, *cbargs, **cbkwargs),
            defer=fun,
        )

    @classmethod
    def now(
        cls,
        source: T,
        mapper: Callable[[T], R] = must_bind,
    ) -> "InputBinding[T, R]":
        """
        replaced by `immediate`
        """
        return cls.immediate(source, mapper)  # pragma: no cover

    @classmethod
    def deferred(
        cls,
        source: ContextVar[T],
        mapper: BindingMapperType[T, R] = must_bind,
        get_first: bool = True,
    ) -> "InputBinding[T, R]":
        """
        replaced by `contextual`
        """
        return cls.contextual(source, mapper, get_first)  # pragma: no cover


class UnboundInputException(Exception):
    """
    Exception raised from InputBinding if an exception is raised in the mapper.
    """

    _TMPLT_MSG = "Failure to bind InputBinding with source={source}, mapper={mapper}, defer={defer}"

    def __init__(self, binding: "InputBinding", *args):
        self.binding = binding
        super().__init__(*args)

    def __str__(self) -> str:  # pragma: no cover
        source, mapper, defer = (
            self.binding.source,
            self.binding.mapper,
            self.binding.defer,
        )
        return self._TMPLT_MSG.format(source=source, mapper=mapper, defer=defer)


class UndefinedInputException(Exception):
    """
    Utility exception for library defined mappers when an input cannot be retrieved.
    """


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
            raise UndefinedInputException(
                "Source stage table is undefined... how could I get a stage output?"
            )
        elif source is UNBOUND:
            return self.default
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
                case _:  # pragma: no cover
                    raise Exception("what the f")


@define
class KeyedInputMapper(Generic[T, R]):
    from_key: str = field()
    default: T = field(default=INPUT_NOT_FOUND)
    required: bool = field(default=True)

    def __call__(self, source: Mapping[str, Any] | UNBOUND_T) -> T:
        match (source is UNBOUND, self.required):
            case (True, True):
                raise UndefinedInputException(
                    "Source mapping is undefined... how could I get a keyed input?"
                )
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


def _prepare_inputs_and_context(
    inputs: StageInputs,
) -> tuple[dict[str, Any], dict[str, Any]]:
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
    logged_inputs = {}
    for input_mapping in inputs:
        inputdict, logged = input_mapping.get(), input_mapping.logged
        prepared_inputs.update(inputdict)
        if logged:
            logged_inputs.update(inputdict)

    return prepared_inputs, logged_inputs


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
    """

    function: Callable[P, R]
    inputs: StageInputs = field(factory=list)
    has_run: bool = field(default=False)
    outputs: StageOutputs = field(factory=dict)
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
        inputs, logctx = _prepare_inputs_and_context(self.inputs)
        inputs.update(kwargs)
        with logging_context(**logctx):
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

    def __repr__(self) -> str:  # pragma: no cover
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

    def __str__(self) -> str:  # pragma: no cover
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
