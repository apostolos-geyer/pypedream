# pypedream
<img src="./_docs/source/_static/pypedream.webp" width=150 height=150>


A lightweight library for workflows in Python. Aims to remove the burden
of setting up things like structured logging, configurability, access to shared context, and more, without using an orchestration platform.

## Who is this for?

*Currently, nobody. It's not finished, and the interface might change multiple times as I iterate towards an optimal implementation and developer experience. But it's co-op season, and I want recruiters to see that I'm active. Don't learn how to use this until the README says it's done. I'm not adhering to open/closed principles right now—everything is open. I digress.*

This library might be useful for:

- Those iterating quickly on a pipeline who don't want to worry about deploying Airflow, Dagster, or similar platforms, and/or don't need all their features yet.
- Developers who need to run the same processes on data and require modularity, version control, and robustness beyond `print` statements and `report_generator_notebook_v42069_may_24.ipynb` with a `utils.py` file.
- Anyone whose pipelines do not need to be constantly running, can operate on their own hardware, and who prefer to avoid setting up an entire workflow orchestration platform.

### Motivation

I was working on a client project involving continuous data processing through different layers, similar to a medallion architecture, to support multiple analytical processes and report generation. We needed a way to define workflows that were:

- Easily modifiable in case of issues or new requirements.
- Observable, with summaries of pipelines in various formats, structured logging, etc.
- Configurable via user-set parameters before running.
- Conducive to modularity and version control.

While these workloads might scale and run regularly in the future, potentially using Airflow, they don't need to right now—and they might never. Personally, I enjoy setting up Airflow for local development, complete with shell scripts for machine configuration and `docker-compose`. However, my team consists of me and my dad. My dad is a brilliant statistician and consultant and more technical than most people, but he uses Anaconda Navigator—i.e, he does not revel in configuring environments and setting up platforms, he just wants to focus on the analysis logic, and rightly so.

Thus, we needed a framework that sits in the middle, requiring nothing beyond Python.

The initial code I wrote seemed potentially useful to others, so I started turning it into pypedream.

## Usage

Suppose you have the following functions:

```python
# mymodule.py
from datetime import date
from typing import Any

def get_data(source: Any, for_day: date) -> Any:
    return f"{source} for day {for_day}"

def combine_data(data1: Any, data2: Any) -> Any:
    return f"{data1} and {data2}"

def validate_data(new_data: Any, sources: list[Any]) -> bool:
    return all(source in new_data for source in sources)
```

You can chain them together like this:

```python
import pypedream
from pypedream import (
    Pipeline,
    Parameters,
    Stage,
    Input,
    InputBinding,
    KeyedInputMapper,
    DependencyInputMapper,
    context
)

pipeline = pypedream.Pipeline(
    "my pipeline",
    Parameters.define(
        source1='your_mom',
        source2='your_girlfriend'
    ),
    stages={
        "get_data1": Stage(
            get_data,
            inputs=[
                Input(as_arg="source", bind=InputBinding.deferred(context.PARAMETERS, mapper=KeyedInputMapper(from_key="source2"))),
                Input(as_arg="for_day", bind=InputBinding.now(date.today())),
            ]
        ),
        "get_data2": Stage(
            get_data,
            inputs=[
                Input(as_arg="source", bind=InputBinding.deferred(context.PARAMETERS, mapper=KeyedInputMapper(from_key="source1"))),
                Input(as_arg="for_day", bind=InputBinding.now(date.today())),
            ]
        ),
        "combine_data": Stage(
            combine_data,
            inputs=[
                Input(as_arg="data1", bind=InputBinding.deferred(context.STAGES, mapper=DependencyInputMapper(from_stage="get_data1"))),
                Input(as_arg="data2", bind=InputBinding.deferred(context.STAGES, mapper=DependencyInputMapper(from_stage="get_data2"))),
            ]
        ),
        "validate_data": Stage(
            validate_data,
            inputs=[
                Input(as_arg="new_data", bind=InputBinding.deferred(context.STAGES, mapper=DependencyInputMapper(from_stage="combine_data"))),
                Input(as_arg="sources", bind=InputBinding.deferred(context.PARAMETERS, mapper=lambda params: [params["source1"], params["source2"]]))
            ]
        )
    }
)
```

Cool? Cool. Until I reinvent the interface and forget to update the README.

## Roadmap

- [x] Function wrapper that can inject inputs from other function wrappers and map outputs (Stage object)
- [x] Pipeline object to use the function wrappers and provide variables to them (Pipeline object, which might be removed)
- [ ] Logging configuration
- [x] Specify a dependency on shared state that only exists within the context of a pipeline run (using contextvars)
- [ ] Custom output stream object that replaces stdout at pipeline runtime so prints from stage functions get redirected into logging
- [x] Working linear pipeline
- [ ] Explicitly define dependencies between stages and order of execution
- [ ] Better ways to chain stages (currently only linear); support for higher-order patterns and concurrency patterns like map, reduce, broadcast, etc.
- [ ] Jupyter notebook UI
- [ ] Airflow compatibility: convert a pypedream workflow into an Airflow DAG
- [ ] Potentially replace everything and start over
- [ ] Possibly make it more functional and use result types and optionals
- [ ] Maybe use Scala instead (just kidding)
