About pypedream
===============

A lightweight library for workflows in Python. Aims to remove the burden
of setting up things like structured logging, configurability, access to shared context, and more, without using an orchestration platform.

Who is this for?
================

*Currently, nobody. It's not finished, and the interface might change multiple times as I iterate towards an optimal implementation and developer experience. But it's co-op season, and I want recruiters to see that I'm active. Don't learn how to use this until the README says it's done. I'm not adhering to open/closed principles right now—everything is open. I digress.*

This library might be useful for:

- Those iterating quickly on a pipeline who don't want to worry about deploying Airflow, Dagster, or similar platforms, and/or don't need all their features yet.
- Developers who need to run the same processes on data and require modularity, version control, and robustness beyond `print` statements and `report_generator_notebook_v42069_may_24.ipynb` with a `utils.py` file.
- Anyone whose pipelines do not need to be constantly running, can operate on their own hardware, and who prefer to avoid setting up an entire workflow orchestration platform.

Motivation
==========

I was working on a client project involving continuous data processing through different layers, similar to a medallion architecture, to support multiple analytical processes and report generation. We needed a way to define workflows that were:

- Easily modifiable in case of issues or new requirements.
- Observable, with summaries of pipelines in various formats, structured logging, etc.
- Configurable via user-set parameters before running.
- Conducive to modularity and version control.

While these workloads might scale and run regularly in the future, potentially using Airflow, they don't need to right now—and they might never. Personally, I enjoy setting up Airflow for local development, complete with shell scripts for machine configuration and `docker-compose`. However, my team consists of me and my dad. 
My dad is a brilliant statistician and consultant and more technical than most people, but he uses Anaconda Navigator—i.e, he does not revel in configuring environments and setting up platforms, he just wants to focus on the analysis logic, and rightly so.

Thus, we needed a framework that sits in the middle, requiring nothing beyond Python.

The initial code I wrote seemed potentially useful to others, so I started turning it into pypedream.

Usage
=====

Suppose you have the following functions:

.. code-block:: python

    from datetime import date
    from typing import Any

    from pypedream.logging import info

    def get_data(source: Any, for_day: date) -> Any:
        info(f"Getting data from {source} for day {for_day}")
        return f"{source} for day {for_day}"


    def combine_data(data1: str, data2: str) -> str:
        info(f"Combining data {data1} and {data2}")
        return f"{data1} and {data2}"


    def validate_data(new_data: str, sources: list[str]) -> bool:
        info("Validating data against sources", data=new_data, sources=sources)
        return all(source in new_data for source in sources)

You can chain them together like this:

.. code-block:: python

    from pathlib import Path
    import pypedream
    from pypedream import (
        Pipeline,
        Parameters,
        Stage,
        Input,
        InputBinding,
        KeyedInputMapper,
        ctx,
    )

    from pypedream.input import dependency, known, param

    log_dir = Path.cwd() / "logs"
    log_dir.mkdir(exist_ok=True)
    pipeline = Pipeline(
        "ok enough",
        Parameters.define(source1="your_mom", source2="your_girlfriend"),
        stages={
            **{
                f"get_data{i}": Stage(
                    get_data,
                    inputs=[
                        param(f"source{i}", as_arg="source"),
                        known(date.today(), as_arg="for_day"),
                    ],
                )
                for i in (1, 2)
            },
            "combine_data": Stage(
                combine_data,
                inputs=[dependency(f"get_data{i}", as_arg=f"data{i}") for i in (1, 2)],
            ),
            "validate_data": Stage(
                validate_data,
                inputs=[
                    dependency("combine_data", as_arg="new_data"),
                    Input(
                        as_arg="sources",
                        bind=InputBinding.deferred(
                            ctx.PARAMETERS,
                            mapper=lambda params: [params["source1"], params["source2"]],
                        ),
                        logged=True,
                    ),
                ],
            ),
        },
        log_settings=LoggerSettings(
            "ok_enough",
            log_dir=ldir,
        )
    )

Just like that, you have structured json logging to a file, pretty printing logs to the console, access
to pipeline local state with contextvars that are enclosed within the pipeline, packaged in a highly
expressive (and improving) interface.

That is, until I decide all of this is wrong and change everything.

Roadmap
=======

- ✅ Function wrapper that can inject inputs from other function wrappers and map outputs (Stage object)
- ✅ Pipeline object to use the function wrappers and provide variables to them (Pipeline object, which might be removed)
- ✅ Logging configuration
- ✅ Specify a dependency on shared state that only exists within the context of a pipeline run (using contextvars)
- ❌ (cancelled cause this is stupid) Custom output stream object that replaces stdout at pipeline runtime so prints from stage functions get redirected into logging
- ✅ Working linear pipeline
- ✅ more concise abstractions over inputs and inputbindings (pypedream.inputs module)
- 🔲 Explicitly define dependencies between stages and order of execution
- 🔲 Better ways to chain stages (currently only linear); support for higher-order patterns and concurrency patterns like map, reduce, broadcast, etc.
- 🔲 Jupyter notebook UI
- 🔲 Airflow compatibility: convert a pypedream workflow into an Airflow DAG
- 🔲 Potentially replace everything and start over
- 🔲 Possibly make it more functional and use result types and optionals
- 🔲 Maybe use Scala instead (just kidding)