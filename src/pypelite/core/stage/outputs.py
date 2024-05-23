from enum import Flag, auto
from functools import cached_property
from typing import Any, Mapping, ParamSpec, Sequence, TypeVar

from attrs import field, frozen

from pypelite.core.stage.annotation import (
    StageOutputs,
)

P = ParamSpec("P")
R = TypeVar("R")
T = TypeVar("T")

__all__ = [
    "DEFAULT_OUTPUT_KEY",
    "OutputMapperBehaviour",
    "STRICT",
    "CHILL",
    "PRESERVE",
    "WARN_UNEXPECTED",
    "DEFAULT_OUTPUT_MAPPER",
    "SequentialOutputMapper",
    "KeyedOutputMapper",
]

DEFAULT_OUTPUT_KEY = "return"


# ------------------------------
# OUTPUT MAPPERS
# ------------------------------
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
        return f"SequenceOutputMapper(keys={self.keys})"

    @cached_property
    def __str__(self) -> str:
        string = "SequenceOutputMapper(\n"
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
