import pytest

from datetime import date, timedelta
from pypedream import Pipeline, Parameters, Stage, Input, InputBinding, ctx
from pypedream.input import immediate, param, dependency


@pytest.fixture
def rizz_pipeline():
    pipeline = Pipeline(
        "rizz",
        Parameters.define(source1="your_mom", source2="your_girlfriend"),
        stages={
            **{
                f"rizz{i}": Stage(
                    (lambda source, day: f"rizzed {source} on {day}"),
                    inputs=[
                        param(f"source{i}", as_arg="source"),
                        immediate(date.today() - timedelta(days=2 - i), as_arg="day"),
                    ],
                )
                for i in (1, 2)
            },
            "combine": Stage(
                (lambda rizzed1, rizzed2: f"{rizzed1} and {rizzed2}"),
                inputs=[
                    dependency("rizz1", as_arg="rizzed1"),
                    dependency("rizz2", as_arg="rizzed2"),
                ],
                output_mapper=lambda x: {"combined_rizzed": x},
            ),
            "ensure_all_rizzed": Stage(
                (
                    lambda combined_rizzed, sources: all(
                        rizzed in combined_rizzed for rizzed in sources
                    )
                ),
                inputs=[
                    dependency(
                        "combine",
                        as_arg="combined_rizzed",
                        from_output="combined_rizzed",
                    ),
                    Input(
                        as_arg="sources",
                        bind=InputBinding.contextual(
                            ctx.PARAMETERS,
                            mapper=lambda params: [
                                params["source1"],
                                params["source2"],
                            ],
                        ),
                        logged=True,
                    ),
                ],
            ),
        },
        log_settings=None,
    )

    return pipeline


def test_rizz_pipeline(rizz_pipeline):
    result = rizz_pipeline.run()
    assert result == {
        "rizz1": f"rizzed your_mom on {date.today() - timedelta(days=1)}",
        "rizz2": f"rizzed your_girlfriend on {date.today()}",
        "combine": f"rizzed your_mom on {date.today() - timedelta(days=1)} and rizzed your_girlfriend on {date.today()}",
        "ensure_all_rizzed": True,
    }
