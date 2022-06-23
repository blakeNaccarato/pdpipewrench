
Disclaimer
----------

This project is no longer maintained. If you are here because you have a project idea and wanted to publish it as `pdpipewrench` on PyPI, contact me at blake.naccarato+pdpipewrench@gmail.com and we can talk.

It was initially conceptualized as a way to outfit [pdpipe](https://github.com/pdpipe/pdpipe) pipelines from configuration files, allowing for Pandas pipeline orchestration with minimal code. I have since adopted a less aggressive tact over in [boilerdata](https://github.com/blakeNaccarato/boilerdata), where I still separate configuration out into YAML files (constants, file paths, pipeline function arguments, etc.), but pipeline logic is handled in `pipeline.py`. I have also done away with using `pdpipe` in this approach, as it doesn't lend itself particularly well to [ETL](https://en.wikipedia.org/wiki/Extract,_transform,_load). Besides, my data processing need is not quite the "flavor" of statistical data science type approaches supported by `pdpipe`.

This new approach maintains the benefits of writing logic in Python, while allowing configuration in files. I am using [Pydantic](https://github.com/samuelcolvin/pydantic) as the interface between my configs and my logic, which allows me to specify allowable values with `Enums` and other typing constructs. Expressing allowable configurations with Pydantic allows for generation of schema for your config files, raising errors on typos or missing keys, for example. I also specify the "shape" of my input and output data in configs, and validate my dataframes with [pandera](https://github.com/pandera-dev/pandera). Once these components are in place, it is easy to implement new functionality in the pipeline.

The limitations of the approach taken in `PdPipeWrench` are sidestepped by my approach in `boilerdata`, and so I have decided to archive this repository.

PdPipeWrench
============

YAML-configurable Pandas pipelines.

The `pdpipewrench` package reads input data, generates pipeline stages, and writes
output data entirely from the information supplied in a YAML configuration file. In
addition, custom-made or module-specific functions may be wrapped into pipeline stages
as specified in the YAML. Keyword arguments to such functions are also specified in
YAML, which sidesteps the problem of hard coding parameters into numerous `*.py` files
for different datasets, each slightly different than the last.

Related
-------

- [altescy/pdpcli](https://github.com/altescy/pdpcli): YAML-configurable Pandas pipelines with attention given to CLI support.
- [neilbartlett/datapipeliner](https://github.com/neilbartlett/datapipeliner): YAML-configurable Pandas pipelines with attention given to Data Version Control (DVC) needs.

Project Philosophy
------------------

I initially built this because I realized that other "data pipeline" products like Luigi, Airflow, and the like were not really designed for processing the occasional CSV dataset consisting of sensor data and the like, the kind of data that comes out of my graduate thermal engineering research. Luigi and Airflow are great for long-running pipelines that handle customer data in the "data engineering" space, communicating with SQL databases, S3 buckets, data warehouses, and data lakes. I wanted a lower-complexity system for orchestrating pipelines on my experimental data, one that would confine program logic to Python, and pipeline specification to YAML.

I thought that this task would be easy enough, but it has proven difficult to make `pdpipewrench` expressive enough to handle arbitrary pipelines, especially in obtaining intermediate results from the pipeline. `pdpipewrench` works well enough for loading data from a CSV, applying a series of transformations to the data, and then dumping the resulting data back to CSV. I have used it for many-to-many and many-to-one pipelines. I find myself applying multiple pipelines in separate cells of a Jupyter notebook, and hooking up the sink of one pipeline as the source of another. This works fine, but is clunky.

This may turn out to have been a pipe dream after all (heh). Perhaps I am better off by changing my upstream processes, which currently generate CSVs of experimental data, to write to a database format instead. This would allow me to leverage existing, mature pipeline orchestration tools in the data engineering space, even if they aren't strictly designed for my use case. I welcome any feedback that you may have for me, if you have any ideas feel free to open a thread in the "Discussions" tab of this repo!

Installation
------------

    pip install pdpipewrench

Requirements
------------

This package manages YAML configurations with `confuse`, which itself depends on
`pyYAML`. Pipeline stages and pipelines are generated with `pdpipe`, and `engarde` is an
optional dependency for `verify_all`-, `verify_any`-, and `engarde`-type stages.

Details
-------

All aspects of a pipeline are defined in `config.yaml`. This file contains information
about `sources`, files from which the data is drawn, `pipelines` and their stages, and
the `sinks`, files to which the transformed data is written. Custom-made functions may
be defined in a standard `*.py` file/module, which must take a `pandas.DataFrame` as
input and return a `pandas.DataFrame` as output. Pipeline stages are generated from
these custom functions by specifying them and their keyword arguments in `config.yaml`.

The file `config.yaml` controls all aspects of the pipeline, from data discovery, to
pipeline stages, to data output. If the environment variable `PDPIPEWRENCHDIR` is not
specified, then then it will be set to the current working directory. The file
`config.yaml` should be put in the `PDPIPEWRENCHDIR`, and data to be processed should be
in that directory or its subdirectories.

Example
-------

The directory structure of this example is as follows:

    example/
        config.yaml
        custom_functions.py
        example.py
        raw
            products_storeA.csv
            products_storeB.csv
        output
            products_storeA_processed.csv
            products_storeB_processed.csv

The contents of `config.yaml` is as follows (paths are relative to the location of
`config.yaml`, i.e. the `PDPIPEWRENCHDIR`):

    sources:
      example_source:
        file: raw/products*.csv
        kwargs:
          usecols:
            - items
            - prices
            - inventory
        index_col: items

    sinks:
      example_sink:
        file: output/*_processed.csv

    pipelines:
      example_pipeline:

      - type: transform
          function: add_to_col
          kwargs:
            col_name: prices
            val: 1.5
          staging:
            desc: Adds $1.5 to column 'prices'
            exmsg: Couldn't add to 'prices'.

        - type: pdpipe
          function: ColDrop
          kwargs:
            columns: inventory
          staging:
            exraise: false

        - type: verify_all
          check: high_enough
          kwargs:
            col_name: prices
            val: 19
          staging:
            desc: Checks whether all 'prices' are over $19.

The module `custom_functions.py` contains:

    custom_functions.py

        def add_to_col(df, col_name, val):
            df.loc[:, col_name] = df.loc[:, col_name] + val
            return df

        def high_enough(df, col_name, val):
            return df.loc[:, col_name] > val

Finally, the contents of the file `example.py`:

    import custom_functions
    import pdpipewrench as pdpw

    src = pdpw.Source("example_source")  # generate the source from `config.yaml`
    snk = pdpw.Sink("example_sink")  # generate the sink from `config.yaml`.

    # generate the pipeline from `config.yaml`.
    line = pdpw.Line("example_pipeline", custom_functions)

    # connect the source and sink to the pipeline, print what the pipeline will do, then run
    # the pipeline, writing the output to disk. capture the input/output dataframes if desired.
    pipeline = line.connect(src, snk)
    print(pipeline)
    (dfs_in, dfs_out) = line.run()

Running `example.py` generates `src`, `snk`, and `line` objects. Then, the `src` and
`snk` are connected to an internal `pipeline`, which is a `pdpipe.PdPipeLine` object.
When this pipeline is printed, the following output is displayed:

    A pdpipe pipeline:
    [ 0]  Adds $1.5 to column 'prices'
    [ 1]  Drop columns inventory
    [ 2]  Checks whether all 'prices' are over $19.

The function of this pipeline is apparent from the descriptions of each stage. Some
stages have custom descriptions specified in the `desc` key of `config.yaml`. Stages
of type `pdpipe` have their descriptions auto-generated from the keyword arguments.

The command `line.run()` pulls data from `src`, passes it through `pipeline`, and
drains it to `snk`. The returns `dfs_in` and `dfs_out` show that came in from `src`
and what went to `snk`. In addition to `line.run()`, the first `n` stages of the
pipeline can be tested on file `m` from the source with `line.test(m,n)`.

Output from Example
-------------------

This is  `.\raw\products_storeA.csv` before it is drawn into the source:

| items   |   prices |   inventory | color |
|:--------|---------:|------------:|------:|
| foo     |       19 |           5 |   red |
| bar     |       24 |           3 | green |
| baz     |       22 |           7 |  blue |

This is  `.\raw\products_storeA.csv` after it is drawn into the source with the argument
`usecols = ["items", "prices", "inventory"]` specified in `config.yaml`:

| items   |   prices |   inventory |
|:--------|---------:|------------:|
| foo     |       19 |           5 |
| bar     |       24 |           3 |
| baz     |       22 |           7 |

The output from the pipeline is sent to `.\products_storeA_processed.csv`. The arguments
specified by `config.yaml` have been applied. Namely, `prices` have been incremented by
`1.5`, the `inventory` column has been dropped, and then a check has been made that all
`prices` are over `19`.

| items   |   prices |
|:--------|---------:|
| foo     |     20.5 |
| bar     |     25.5 |
| baz     |     23.5 |

If the `verify_all` step had failed, an exception would be raised, and the items that
did not pass the check would be returned in the exception message. Say, for example,
that the `val` argument was `21` instead of `19`:

    AssertionError: ('high_enough not true for all',
    prices  items        
    foo      20.5)
