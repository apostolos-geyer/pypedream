from functools import cached_property
from typing import Any, Literal, Mapping, ParamSpec, TypeVar

from attrs import define, field

from pypelite.core.stage.annotation import (
    StageInputMapping,
    StageInputs,
    StageKey,
    StageTable,
)

from pypelite.core.stage.outputs import DEFAULT_OUTPUT_KEY

__all__ = [
    "INPUT_NOT_FOUND",
    "DependencyInput",
    "KeyedInput",
    "DefaultInput",
    "bind_inputs",
    "bind_inputs_by_type",
    "prepare_inputs",
]

P = ParamSpec("P")
R = TypeVar("R")
T = TypeVar("T")

INPUT_NOT_FOUND = "INPUT NOT FOUND"


# ------------------------------
# INPUT MAPPINGS
# ------------------------------
@define
class DependencyInput:
    """
    DependencyInput is an implementation of the StageInputMapping protocol that maps an output
    from a stage to an argument in another stage.

    Attributes
    ----------
    required : bool
        whether the input is required. If a required input is not found, the `get` method should raise a meaningful error.

    as_arg : str
        the argument to pass the input as. This should match the name of the argument in the function signature of
        the stage that the input is being passed to.

    from_stage : StageKey
        the key of the stage to get the output from.

    from_output : str
        the key of the output to get from the stage. By default, this is set to `DEFAULT_OUTPUT_KEY`.

    bound : bool
        whether the input is bound to a source. If bound, the `get` method can be called without directly passing the source.

    default : Any
        the default value to use if the output is not found. By default, this is set to `INPUT_NOT_FOUND`.

    Methods
    -------
    get(where: StageTable) -> dict[str, Any]
        maps the input to a dictionary with a single key `as_arg` that maps to the input value.

    bind(source: StageTable) -> None
        binds the input mapping to a source, replacing self.get(where: T) with a closure that contains the source
        passed as where. The source can still be overriden by calling get with an argument.
    """

    required: bool
    as_arg: str
    from_stage: StageKey
    from_output: str = field(default=DEFAULT_OUTPUT_KEY)
    default = field(default=INPUT_NOT_FOUND)
    bound: bool = field(default=False)
    _bound_to: StageTable | None = field(default=None)

    def get(self, where: StageTable | None = None) -> dict[str, Any]:
        """
        maps the input to a dictionary with a single key `as_arg` that maps to the input value.

        Parameters
        ----------
        where : StageTable
            the dictionary of stages to get the input from

        Returns
        -------
        a dictionary with the input mapped to the key `as_arg`

        Raises
        ------
        ValueError
            if the input is required and not found
        """
        match (where, self.bound):
            case (None, False):
                raise ValueError(
                    "Input mapping is not bound and no source was provided."
                )
            case (None, True):
                where = self._bound_to
            case (_, _):
                pass  # use the provided source even if bound

        stage = where.get(self.from_stage, None)

        if stage is None:
            raise ValueError(f"Stage {self.from_stage} not found in source.")

        if not self.required:
            return {self.as_arg: stage.outputs.get(self.from_output, self.default)}

        dependent_stage_ran = stage.has_run
        dependent_stage_has_output = self.from_output in stage.outputs

        if not dependent_stage_ran:
            raise ValueError(
                f"Stage {self.from_stage} has not run yet. Cannot get output {self.from_output}."
            )

        if not dependent_stage_has_output:
            raise ValueError(
                f"Stage {self.from_stage} does not have output {self.from_output}."
            )

        return {self.as_arg: stage.outputs[self.from_output]}

    def bind(self, source: StageTable) -> None:
        """
        Binds the input mapping to a source.

        Parameters
        ----------
        source : StageTable
            the stage table to bind the mapping to
        """
        self.bound = True
        self._bound_to = source


@define
class KeyedInput:
    """
    KeyedInputMapping is an implementation of the StageInputMapping protocol that maps a key in a dictionary
    to an argument in a stage.

    Attributes
    ----------
    required : bool
        whether the input is required. If a required input is not found, the `get` method should raise a meaningful error.

    as_arg : str
        the argument to pass the input as. This should match the name of the argument in the function signature of
        the stage that the input is being passed to.

    from_key : str
        the key of the value to get from the dictionary.

    Methods
    -------
    get(where: Mapping) -> dict[str, Any]
        maps the input to a dictionary with a single key `as_arg` that maps to the input value.

    bind(source: Mapping) -> None
        binds the input mapping to a source, allowing the `get` method to be called without passing the source.
    """

    required: bool
    as_arg: str
    from_key: str
    default: str = field(default=INPUT_NOT_FOUND)
    bound: bool = field(default=False)
    _bound_to: StageTable | None = field(default=None)

    def get(self, where: Mapping[str, Any] | None = None) -> dict[str, Any]:
        """
        maps the input to a dictionary with a single key `as_arg` that maps to the input value.

        Parameters
        ----------
        where : dict[str, Any]
            the dictionary to get the input from

        Returns
        -------
        a dictionary with the input mapped to the key `as_arg`

        Raises
        ------
        ValueError
            if the input is required and not found
        """
        match (where, self.bound):
            case (None, False):
                raise ValueError(
                    "Input mapping is not bound and no source was provided."
                )
            case (None, True):
                where = self._bound_to
            case (_, _):
                pass  # use the provided source even if bound

        if not self.required:
            return {self.as_arg: where.get(self.from_key, self.default)}

        if self.from_key not in where:
            raise ValueError(f"Key {self.from_key} not found in input dictionary.")

        return {self.as_arg: where[self.from_key]}

    def bind(self, source: Mapping[str, Any]) -> None:
        """
        Binds the input mapping to a source.

        Parameters
        ----------
        source : dict[str, Any]
            the dictionary to bind the mapping to
        """
        self.bound = True
        self._bound_to = source


@define
class DefaultInput:
    """
    DefaultInputMapping is an implementation of the StageInputMapping protocol for defining default values for inputs.
    ..maybe a little overkill but this is what happens when we use interfaces lol.

    Attributes
    ----------
    as_arg : str
        the argument to pass the input as. This should match the name of the argument in the function signature of
        the stage that the input is being passed to.

    value : Any
        the value to use.

    Methods
    -------
    get(where: Any) -> dict[str, Any]
        maps the input to a dictionary with a single key `as_arg` that maps to the input value.
        If where is provided then where will be used as the value. Otherwise, the `value` will be used.

    bind(source: Any) -> None
        Replaces the default value with a new one.
    """

    as_arg: str
    value: Any

    @cached_property
    def bound(self) -> Literal[True]:
        """Default inputs are always bound to a source."""
        return True

    @cached_property
    def required(self) -> Literal[False]:
        """
        Default inputs are never required, not that it matters as they'll never fail to get.
        Just here for completeness.
        """
        return False

    def get(self, where: Any | None = None) -> dict[str, Any]:
        """
        maps the input to a dictionary with a single key `as_arg` that maps to the input value.

        Parameters
        ----------
        where : Any
            the source to get the input from

        Returns
        -------
        a dictionary with the input mapped to the key `as_arg`
        """
        if where is not None:
            return {self.as_arg: where}
        return {self.as_arg: self.value}

    def bind(self, source: Any) -> None:
        """
        Binds the input mapping to a source.

        Parameters
        ----------
        source : Any
            the source to bind the mapping to
        """
        self.value = source


def bind_inputs(
    inputs: list[StageInputMapping[T]], source: T
) -> list[StageInputMapping[T]]:
    """
    Bind all input mappings to a source.

    Parameters
    ----------
    inputs : list[StageInputMapping]
        the list of input mappings to bind

    source : T
        the source to bind the input mappings to

    Returns
    -------
    a list of input mappings with the sources bound
    """
    for input_mapping in inputs:
        input_mapping.bind(source)

    return inputs


def bind_inputs_by_type(
    inputs: StageInputs, sources: Mapping[type[StageInputMapping[T]], T]
) -> StageInputs:
    """
    Bind input mappings to sources by type, ignoring any inputs that do not have a source.

    Parameters
    ----------
    inputs : list[StageInputMapping]
        the list of input mappings to bind

    sources : dict[type[StageInputMapping[T]], T]
        the dictionary of sources to bind the input mappings to

    Returns
    -------
    a list of input mappings with the sources bound
    """
    for input_mapping in inputs:
        mapping_type = type(input_mapping)
        source = sources.get(mapping_type, None)
        if source is not None:
            input_mapping.bind(source)

    return inputs


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
