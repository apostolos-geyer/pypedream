import random
from contextvars import ContextVar

import pytest

from pypedream.core.stages import (
    DEFAULT_OUTPUT_KEY,
    INPUT_NOT_FOUND,
    UNBOUND,
    InputBinding,
    KeyedInputMapper,
    DependencyInputMapper,
    SequentialOutputMapper,
    KeyedOutputMapper,
    Stage,
    UnboundInputException,
    UndefinedInputException,
    STRICT,
    CHILL,
    PRESERVE,
    WARN_UNEXPECTED,
    _prepare_inputs_and_context,
    Input,
)

# Assuming the SequentialOutputMapper and OutputMapperBehaviour have been correctly imported and set up


def test_inputs_and_preparation():
    input1 = Input(as_arg="x", bind=InputBinding.immediate(10))
    input2 = Input(as_arg="y", bind=InputBinding.immediate(20), logged=True)
    input3 = Input(as_arg="z", bind=InputBinding.immediate(30), logged=True)

    inputs = [input1, input2, input3]

    all_inputs, logged_inputs = _prepare_inputs_and_context(inputs)
    assert all_inputs == {"x": 10, "y": 20, "z": 30}
    assert logged_inputs == {"y": 20, "z": 30}


def test_sequential_output_mapper_strict():
    mapper = SequentialOutputMapper(keys=["a", "b", "c"], behaviour=STRICT)
    # Correct length sequence
    assert mapper(["1", "2", "3"]) == {"a": "1", "b": "2", "c": "3"}
    # Incorrect length sequence, should raise ValueError
    with pytest.raises(ValueError):
        mapper(["1", "2"])


def test_sequential_output_mapper_chill():
    mapper = SequentialOutputMapper(keys=["a", "b", "c"], behaviour=CHILL)
    # More items than keys
    assert mapper(["1", "2", "3", "4"]) == {"a": "1", "b": "2", "c": "3"}
    # Less items than keys
    assert mapper(["1", "2"]) == {"a": "1", "b": "2"}


def test_sequential_output_mapper_preserve():
    mapper = SequentialOutputMapper(keys=["a", "b", "c"], behaviour=PRESERVE)
    # More items than keys, should include extra under '_extra'
    assert mapper(["1", "2", "3", "4"]) == {
        "a": "1",
        "b": "2",
        "c": "3",
        "_extra": ["4"],
    }
    # Exact match
    assert mapper(["1", "2", "3"]) == {"a": "1", "b": "2", "c": "3"}


def test_sequential_output_mapper_chill_with_warning():
    mapper = SequentialOutputMapper(keys=["a", "b"], behaviour=CHILL | WARN_UNEXPECTED)
    with pytest.warns():
        assert mapper(["1", "2", "3"]) == {"a": "1", "b": "2"}


def test_sequential_output_mapper_preserve_with_warning():
    mapper = SequentialOutputMapper(keys=["a"], behaviour=PRESERVE | WARN_UNEXPECTED)
    with pytest.warns():
        assert mapper(["1", "2"]) == {"a": "1", "_extra": ["2"]}


def test_only_warn_invalid():
    with pytest.raises(ValueError):
        SequentialOutputMapper(keys=["a", "b"], behaviour=WARN_UNEXPECTED)


def test_keyed_output_mapper_strict():
    mapper = KeyedOutputMapper(
        keys={"input1": "output1", "input2": "output2"}, behaviour=STRICT
    )
    # All keys present
    assert mapper({"input1": "1", "input2": "2"}) == {"output1": "1", "output2": "2"}
    # Missing key should raise error
    with pytest.raises(ValueError):
        mapper({"input1": "1"})


def test_keyed_output_mapper_chill():
    mapper = KeyedOutputMapper(
        keys={"input1": "output1", "input2": "output2"}, behaviour=CHILL
    )
    # Extra keys should be ignored
    assert mapper({"input1": "1", "input2": "2", "input3": "3"}) == {
        "output1": "1",
        "output2": "2",
    }
    # Missing keys should be ignored
    assert mapper({"input1": "1"}) == {"output1": "1"}


def test_keyed_output_mapper_preserve():
    mapper = KeyedOutputMapper(keys={"input1": "output1"}, behaviour=PRESERVE)
    # Extra keys should be preserved
    assert mapper({"input1": "1", "input2": "2"}) == {"output1": "1", "input2": "2"}


def test_keyed_output_mapper_chill_with_warning():
    mapper = KeyedOutputMapper(
        keys={"input1": "output1"}, behaviour=CHILL | WARN_UNEXPECTED
    )
    with pytest.warns():
        assert mapper({"input1": "1", "input2": "2"}) == {"output1": "1"}


def test_keyed_output_mapper_preserve_with_warning():
    mapper = KeyedOutputMapper(
        keys={"input1": "output1"}, behaviour=PRESERVE | WARN_UNEXPECTED
    )
    with pytest.warns():
        assert mapper({"input1": "1", "input2": "2"}) == {"output1": "1", "input2": "2"}


@pytest.fixture
def emotional_stage():
    return Stage(
        (
            lambda: (
                ("happy", "excited", "fun"),
                ("sad", "bored", "tired"),
                ("freaky 👅", "rizzy", "nettspend"),
            )
        ),
        output_mapper=SequentialOutputMapper(["good", "bad", "ohio"]),
    )


def emotional_stage_in_table_ran():
    stage = Stage(
        (
            lambda: (
                ("happy", "excited", "fun"),
                ("sad", "bored", "tired"),
                ("freaky 👅", "rizzy", "nettspend"),
            )
        ),
        output_mapper=SequentialOutputMapper(["good", "bad", "ohio"]),
    )
    stage.run()
    return {"emotional": stage}


@pytest.fixture
def random_stage():
    return Stage(
        (lambda: random.randint(0, 100)),
    )


def test_stage(random_stage):
    x = random_stage.run()
    assert random_stage.outputs == {DEFAULT_OUTPUT_KEY: x}


def test_sequential_output_mapper(emotional_stage):
    a, b, c = emotional_stage.run()
    assert emotional_stage.has_run
    assert a == ("happy", "excited", "fun")
    assert b == ("sad", "bored", "tired")
    assert c == ("freaky 👅", "rizzy", "nettspend")

    assert emotional_stage.outputs == {
        "good": a,
        "bad": b,
        "ohio": c,
    }

    emotional_stage.reset()
    assert not emotional_stage.has_run
    assert emotional_stage.outputs == {}


@pytest.mark.parametrize(
    "stage_table, mapper, expected, raises, matches",
    [
        (
            emotional_stage_in_table_ran(),
            DependencyInputMapper("emotional", "good"),
            ("happy", "excited", "fun"),
            None,
            None,
        ),
        (
            {},
            DependencyInputMapper("emotional", "good"),
            None,
            UndefinedInputException,
            "not found",
        ),
        (
            UNBOUND,
            DependencyInputMapper("emotional", "good"),
            None,
            UndefinedInputException,
            "is undefined",
        ),
        (
            emotional_stage_in_table_ran(),
            DependencyInputMapper("emotional", "wrong_key"),
            None,
            UndefinedInputException,
            "completed and did not produce output",
        ),
        (
            emotional_stage_in_table_ran(),
            DependencyInputMapper("wrong_stage", "good"),
            None,
            UndefinedInputException,
            "not found in source",
        ),
        (
            {"a_stage": Stage(lambda: 69)},
            DependencyInputMapper("a_stage"),
            None,
            UndefinedInputException,
            "has not run",
        ),
        (
            emotional_stage_in_table_ran(),
            DependencyInputMapper("emotional", "wrong_key", required=False),
            INPUT_NOT_FOUND,
            None,
            None,
        ),
        (
            emotional_stage_in_table_ran(),
            DependencyInputMapper("wrong_stage", "good", required=False),
            INPUT_NOT_FOUND,
            None,
            None,
        ),
        (
            UNBOUND,
            DependencyInputMapper("foo", "bar", required=False),
            INPUT_NOT_FOUND,
            None,
            None,
        ),
    ],
)
def test_dependency_input_mapper(stage_table, mapper, expected, raises, matches: str):
    match (raises, matches):
        case (None, None):
            result = mapper(stage_table)
            assert result == expected
        case _:
            with pytest.raises(raises) as exc_info:
                mapper(stage_table)

            assert matches in exc_info.exconly()


#######################
# JUST INPUT BINDINGS #
#######################


@pytest.fixture
def sample_function():
    def func(x):
        return x * 2

    return func


@pytest.mark.parametrize(
    "immediate, mapper, expected, raises, raised_from",
    [
        (10, None, 10, None, None),  # normal integer
        (0, None, 0, None, None),  # zero as a valid integer
        ("", None, "", None, None),  # empty string
        (None, None, None, None, None),  # None value
        (UNBOUND, None, None, UnboundInputException, ValueError),
        (UNBOUND, lambda x: x if x is not UNBOUND else 0, 0, None, None),
        # we also cover KeyedInputMapper here just for shits and giggles
        ({"sally": 10, "joe": 20}, KeyedInputMapper("sally"), 10, None, None),
        (
            {"sally": 10, "joe": 20},
            KeyedInputMapper("jim", default=-1, required=False),
            -1,
            None,
            None,
        ),
        (
            {"sally": 10, "joe": 20},
            KeyedInputMapper("jim", default=-1, required=True),
            -1,
            UnboundInputException,
            UndefinedInputException,
        ),
        (
            UNBOUND,
            KeyedInputMapper("jim", required=True),
            None,
            UnboundInputException,
            UndefinedInputException,
        ),
        (
            UNBOUND,
            KeyedInputMapper("jim", required=False),
            INPUT_NOT_FOUND,
            None,
            None,
        ),
    ],
)
def test_immediate_binding(immediate, mapper, expected, raises, raised_from):
    def _get_immediate_binding():
        return (
            InputBinding.immediate(immediate)
            if mapper is None
            else InputBinding.immediate(immediate, mapper=mapper)
        )

    match (raises, raised_from):
        case (None, None):
            assert _get_immediate_binding()() == expected, "Immediate binding failed."

        case (_, None):
            with pytest.raises(raises):
                _get_immediate_binding()()

        case (_, _):
            with pytest.raises(raises) as exc_info:
                _get_immediate_binding()()

            assert isinstance(
                exc_info.value.__cause__, raised_from
            ), f"Exception {raises} should have cause {raised_from}."


ctx_nodefault = ContextVar("test_var_without_default")
ctx_unbound = ContextVar("test_var_with_default", default=UNBOUND)
ctx_number = ContextVar("test_var_with_number", default=10)


@pytest.mark.parametrize(
    "ctxvar, get_first, mapper, output, raises, raised_from",
    [
        (ctx_nodefault, True, None, None, UnboundInputException, LookupError),
        (ctx_unbound, True, None, None, UnboundInputException, ValueError),
        (ctx_number, True, None, 10, None, None),
        (ctx_nodefault, False, lambda x: x.get(-1), -1, None, None),
    ],
)
def test_contextual_binding(ctxvar, get_first, mapper, output, raises, raised_from):
    def _get_contextual_binding():
        return (
            InputBinding.contextual(ctxvar, get_first=get_first)
            if mapper is None
            else InputBinding.contextual(ctxvar, get_first=get_first, mapper=mapper)
        )

    match (raises, raised_from):
        case (None, None):
            assert _get_contextual_binding()() == output, "Contextual binding failed."

        case (_, None):
            with pytest.raises(raises):
                _get_contextual_binding()()

        case (_, _):
            with pytest.raises(raises) as exc_info:
                _get_contextual_binding()()

            assert isinstance(
                exc_info.value.__cause__, raised_from
            ), f"Exception {raises} should have cause {raised_from}."


@pytest.mark.parametrize(
    "func_input, expected_output",
    [
        (10, 20),  # simple doubling
        (0, 0),  # zero case
    ],
)
def test_callback_binding(sample_function, func_input, expected_output):
    binding = InputBinding.callback(sample_function, func_input)
    assert (
        binding() == expected_output
    ), f"Callback binding failed for input {func_input}"
