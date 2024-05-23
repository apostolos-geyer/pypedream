from typing import TYPE_CHECKING, Any, ParamSpec, Protocol, TypeAlias, TypeVar

if TYPE_CHECKING:
    from pypelite.core.stage.models import Stage

__all__ = [
    "StageKey",
    "StageInputs",
    "StageOutputs",
    "StageTable",
    "StageOutputMapper",
    "StageInputMapping",
]


T = TypeVar("T")
P = ParamSpec("P")
R = TypeVar("R")

StageKey: TypeAlias = str
StageInputs: TypeAlias = list["StageInputMapping"]
StageOutputs: TypeAlias = dict[str, Any]
StageTable = dict[StageKey, "Stage"]


class StageOutputMapper(Protocol):
    """
    Protocol for defining output mappers for stages.

    Implementations should define a `__call__` method that takes the return value of a stage
    and returns a dictionary of the form {key: value} to be stored as the outputs of the stage.
    """

    def __call__(self, output: Any) -> StageOutputs:
        ...


class StageInputMapping(Protocol[T]):
    # TODO: add more structured behaviour for handling inputs that are not found similar to the output mappers
    """
    Protocol for defining input mappings for stages. The generic type T represents the type of the object
    that will contain the input to be mapped.

    Attributes
    ----------
    as_arg : str
        the argument to pass the input as. This should match the name of the argument in the function signature of
        the stage that the input is being passed to.

    required : bool
        whether the input is required. If a required input is not found, the `get` method should raise a meaningful error.

    bound : bool
        whether the input is bound to a source. If bound, the `get` method can be called without directly passing the source.

    Methods
    -------
    get(where: T) -> dict[str, Any]
        maps the input to a dictionary with a single key `as_arg` that maps to the input value.

    bind(source: T) -> Self
        binds the input mapping to a source, allowing the `get` method to be called without passing the source.
    """

    as_arg: str
    required: bool
    bound: bool

    def get(self, where: T | None = None) -> dict[str, Any]:
        """
        maps the input to a dictionary with a single key `as_arg` that maps to the input value.

        Parameters
        ----------
        where : T | None
            the object to get the input from or None if the input is bound

        Returns
        -------
        a dictionary with the input mapped to the key `as_arg`

        Raises
        ------
        ValueError
            if the input is required and not found
        """
        ...

    def bind(self, source: T) -> "StageInputMapping[T]":
        ...
