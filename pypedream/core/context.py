import sys
from io import TextIOWrapper
from typing import TYPE_CHECKING, Any
from contextvars import ContextVar

if TYPE_CHECKING:
    from pypedream.core.pipelines import Pipeline, Variables, Parameters
    from pypedream.core.stages import Stage, StageTable
else:
    Pipeline = Variables = Parameters = Stage = StageTable = Any

PIPELINE: ContextVar[Pipeline | None] = ContextVar("current_pipeline")
VARIABLES: ContextVar[Variables | None] = ContextVar("current_variables")
PARAMETERS: ContextVar[Parameters | None] = ContextVar("current_parameters")
STAGES: ContextVar[StageTable | None] = ContextVar("current_stages")
STAGE: ContextVar[Stage | None] = ContextVar("current_stage")
STDOUT: ContextVar[TextIOWrapper | None] = ContextVar("stdout")
STDERR: ContextVar[TextIOWrapper | None] = ContextVar("stderr")


def applydefaults():
    PIPELINE.set(None)
    VARIABLES.set(None)
    PARAMETERS.set(None)
    STAGE.set(None)
    STDOUT.set(sys.stdout)
    STDERR.set(sys.stderr)
