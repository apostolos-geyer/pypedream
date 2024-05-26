"""
pypedream.core.context contains context variables used to store the current pipeline, variables, parameters, stages,
stage, stdout, and stderr. It also contains a function to apply defaults to the context variables.

When a stage is run by a pipeline, the pipeline runs it in a context that contains all the information needed to run
the stage and bind its inputs and outputs.

These context variables can also be used within the body of stage functions to access information about the current
pipeline, and modify them. However, this may be dangerous. It is recommended only to modify the `VARIABLES` at
most. Good use of the context variables is in deferred input bindings (bindings of inputs for stages that are
evaluated at stage execution time rather than pipeline construction time)
"""

import logging
from typing import TYPE_CHECKING, Any
from contextvars import ContextVar
from pypedream.core.logs import BASE_LOGGER

if TYPE_CHECKING:
    from pypedream.core.pipelines import Pipeline, Variables, Parameters
    from pypedream.core.stages import Stage, StageTable
else:
    Pipeline = Variables = Parameters = Stage = StageTable = Any

PIPELINE: ContextVar[Pipeline | None] = ContextVar("pipeline")
VARIABLES: ContextVar[Variables | None] = ContextVar("variables")
PARAMETERS: ContextVar[Parameters | None] = ContextVar("parameters")
STAGES: ContextVar[StageTable | None] = ContextVar("stages")
STAGE: ContextVar[Stage | None] = ContextVar("stage")
LOG: ContextVar[logging.Logger | None] = ContextVar("logger")


def applydefaults():
    PIPELINE.set(None)
    VARIABLES.set(None)
    PARAMETERS.set(None)
    STAGE.set(None)
    LOG.set(BASE_LOGGER)


applydefaults()
