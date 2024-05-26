"""
pypedream.ctx re exports the context variables from pypedream.core.context as well as some utility functions for accessing them
directly.
"""
from typing import TypedDict

from pypedream.core.context import LOG, PARAMETERS, PIPELINE, STAGE, STAGES, VARIABLES
from pypedream.core.logs import StdBoundLogger
from pypedream.core.pipelines import Parameters, Pipeline, Variables
from pypedream.core.stages import Stage, StageTable


class PipelineContext(TypedDict):
    pipeline: Pipeline | None
    variables: Variables | None
    parameters: Parameters | None
    stages: StageTable | None
    stage: Stage | None
    logger: StdBoundLogger | None


def pipeline() -> Pipeline | None:
    return PIPELINE.get()


def variables() -> Variables | None:
    return VARIABLES.get()


def parameters() -> Parameters | None:
    return PARAMETERS.get()


def stages() -> StageTable | None:
    return STAGES.get()


def stage() -> Stage | None:
    return STAGE.get()


def logger() -> StdBoundLogger | None:
    return LOG.get()


def pipeline_context() -> PipelineContext:
    return {
        "pipeline": pipeline(),
        "variables": variables(),
        "parameters": parameters(),
        "stages": stages(),
        "stage": stage(),
        "logger": logger(),
    }


__all__ = [
    "pipeline",
    "variables",
    "parameters",
    "stages",
    "stage",
    "logger",
    "pipeline_context",
    "PipelineContext",
    "PARAMETERS",
    "PIPELINE",
    "STAGE",
    "STAGES",
    "VARIABLES",
    "LOG",
]
