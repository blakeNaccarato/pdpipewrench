"""
YAML-configurable Pandas pipelines.
"""

import warnings
from functools import partial, reduce, wraps
from glob import glob
from os import makedirs, path
from types import ModuleType
from typing import ClassVar, List, Union, Tuple, Callable, Dict

import confuse as cf
from engarde import checks as engc
from pandas import DataFrame, concat, read_csv

from . import CONFIG, CONFIG_FOLDERPATH
from . import exceptions as exc

# ignore warnings for missing scikit-learn and nltk extras
with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    import pdpipe as pdp


# * ------------------------------------------------------------------------------ # *
# * HELPER FUNCTIONS * #


def in_config_path(file: str) -> bool:
    """
    Check whether the path to the configuration file is contained in the path to the
    desired input/output file(s).
    """

    return CONFIG_FOLDERPATH.casefold() in file.casefold()


def get_function(function_name: str, module: ModuleType):
    """
    Get a function through successive application of `getattr`. If the parent module
    imports other modules, this will be able to follow a dotted path to functions in the
    imported module.

    Returns
    -------

    `function: Callable`: Callable function or method.
    """

    function_path = [module]
    function_path.extend(function_name.split("."))  # type: ignore
    function = reduce(getattr, function_path)  # type: ignore
    return function


def get_stage_parameters(
    module: ModuleType, stage_config: cf.ConfigView, function_key: str
) -> Tuple[Callable, Dict, Dict]:
    """
    Get parameters for stage building.

    Returns
    -------

    `function: Callable`: Callable function or method.

    `kwargs: Dict`: Keyword arguments to `function`.

    `staging: Dict`: Keyword arguments to the pipeline stage.
    """

    # get transform (returns df)
    function_name = stage_config[function_key].as_str()
    function = get_function(function_name, module)

    # get args for transform and stage
    kwargs = stage_config["kwargs"].get(cf.Template(default={}))
    staging = stage_config["staging"].get(cf.Template(default={}))

    return (function, kwargs, staging)


def df_copy(f: Callable):
    """
    When the partially-specified function, `f` is called, take its only remaining
    argument `df` and return `df.copy()` instead.
    """

    @wraps(f)
    def wrapper(df: DataFrame):
        return f(df.copy())

    return wrapper


# * ------------------------------------------------------------------------------ # *
# * STAGE COMPOSITION * #
# TODO: Catch exceptions giving info about errors in YAML file.


def get_stage_transform(line, stage_config: cf.ConfigView) -> pdp.PdPipelineStage:
    """
    Get a "transform" stage from `config.yaml`.

    Example
    -------

    Given `add_to_col` in a module passed to the `Line` constructor,

        def add_to_col(df, col_name, val):
            df.loc[:, col_name] = df.loc[:, col_name] + val
            return df

    and the following segment of `config.yaml`,

        config.yaml
            ...
            pipelines:
              ...
              example_pipeline:
                ...
                - type: transform
                  function: add_to_col
                  kwargs:
                    col_name: prices
                    val: 1.5
                  staging:
                    desc: Adds 1.5 to column 'prices'
                    exmsg: Couldn't add to 'prices'.
                ...
            ...

    return a `pdpipe.AdHocStage` that applies `add_to_col` to the dataframe with the
    arguments specified in `config.yaml`. When the pipeline containing this stage is
    printed, the stage will appear as `[X] Adds 1.5 to column 'prices'`. If the stage
    fails, the `exmsg`, `Couldn't add to 'prices'.`, is relayed to the user.
    """

    (function, kwargs, staging) = get_stage_parameters(
        line.module, stage_config, "function"
    )
    function = df_copy(partial(function, **kwargs))
    stage = pdp.AdHocStage(function, **staging)
    return stage


def get_stage_pdpipe(line, stage_config: cf.ConfigView) -> pdp.PdPipelineStage:
    """
    Get a "pdpipe" stage from `config.yaml`.

    Example
    -------

    Given the following segment of `config.yaml`,

        config.yaml
            ...
            pipelines:
              ...
              example_pipeline:
                ...
                - type: pdpipe
                  function: ColDrop
                  kwargs:
                    columns: inventory
                  staging:
                    exraise: false
                ...
            ...

    return a `pdpipe.ColDrop` stage that drops the columns specified in `config.yaml`.
    With `exraise: false`, if the column doesn't exist, the dataframe will pass through
    the stage without warning. When the pipeline containing this stage is printed, the
    description will be auto-generated by `pdpipe.ColDrop` to reflect the arguments
    passed.
    """

    (function, kwargs, staging) = get_stage_parameters(pdp, stage_config, "function")
    stage = function(**kwargs, **staging)
    return stage


def get_stage_verify(line, stage_config: cf.ConfigView) -> pdp.PdPipelineStage:
    """
    Get a "verify" stage from `config.yaml`.

    Example
    -------

    Given `high_enough` in a module passed to the `Line` constructor,

        def high_enough(df, col_name, val):
            return df.loc[:, col_name] > val

    and the following segment of `config.yaml`,

        config.yaml
            ...
            pipelines:
              ...
              example_pipeline:
                ...
                - type: verify_all
                  check: high_enough
                  kwargs:
                    col_name: prices
                    val: 19
                  staging:
                    desc: Checks whether all prices are over $19.
                ...
            ...

    return a `pdpipe.AdHocStage` that applies the check `high_enough` using
    `engarde.checks.verify_all` to the dataframe with the arguments specified in
    `config.yaml`. When the pipeline containing this stage is printed, the stage will
    appear as `[X] Checks whether all prices are over $19.`.

    Additionally, `type: verify_any` could've been supplied instead of `type:
    verify_all`.
    """

    (check, kwargs, staging) = get_stage_parameters(line.module, stage_config, "check")
    (verify, _, _) = get_stage_parameters(engc, stage_config, "type")
    function = df_copy(partial(verify, check=check, **kwargs))
    stage = pdp.AdHocStage(function, **staging)
    return stage


def get_stage_engarde(line, stage_config: cf.ConfigView) -> pdp.PdPipelineStage:
    """
    Get an 'engarde' stage from `config.yaml`.

    Example
    -------

    Given the following segment of `config.yaml`,

        config.yaml
            ...
            pipelines:
              ...
              example_pipeline:
                - type: check
                  function: none_missing
                  staging:
                    desc: Checks that there are no missing values (NaNs).
                ...
            ...

    return a `pdpipe.AdHocStage` that applies the check engarde.checks.none_missing` to
    the dataframe. When the pipeline containing this stage is printed, the stage will
    appear as `[X] Checks that there are no missing values (NaNs).`.
    """

    (check, kwargs, staging) = get_stage_parameters(engc, stage_config, "check")
    function = df_copy(partial(check, **kwargs))
    stage = pdp.AdHocStage(function, **staging)
    return stage


# * ------------------------------------------------------------------------------ # *
# * CLASSES * #


class Source:
    """
    Data source for pipelines, built from `config.yaml`.

    Parameters
    ----------

    `name: int, str`: The key that identifies the source in `config.yaml`.

    Example
    -------

    Given the following segment of `config.yaml`,

        config.yaml
          ...
          sources:
            ...
            example_source:
              file: raw/products*.csv
              kwargs:
                usecols:
                  - items
                  - prices
                  - inventory
                index_col: items
            ...
          ...

    running,

        src = Source("example_source")
        src.draw()
        print(src.dfs)

    generates the `Source` object and reads files matching the `file` pattern into
    `pandas.DataFrame` objects, using the `kwargs` specified in the internal call to
    `pandas.read_csv`.
    """

    def __init__(self, name: Union[int, str]):
        """
        Data source for pipelines, built from `config.yaml`.

        Parameters
        ----------

        `name: int, str`: The key that identifies the source in `config.yaml`.
        """

        self.config = CONFIG["sources"][name]
        self.files = sorted(glob(self.config["file"].as_filename()))
        if not in_config_path(self.files[0]):
            raise exc.FileNotInConfigDir(self.files[0], self.config)
        self.kwargs = self.config["kwargs"].get(cf.Template(default={}))

    def draw(self, index: int = None) -> List[DataFrame]:
        """
        Draws data from source file(s).

        Parameters
        ----------

        `index: int, default None`: Draw all source files by default. If `index` is
        specified, then only draw from the file specified by `index`.
        """

        self.dfs: List[DataFrame] = []
        if index is not None:
            self.dfs.append(read_csv(self.files[index], **self.kwargs))
        else:
            for file in self.files:
                self.dfs.append(read_csv(file, **self.kwargs))
        return self.dfs


class Sink:
    """
    Data sink for pipelines, built from `config.yaml`.

    Parameters
    ----------

    `name: int, str`: The key that identifies the sink in `config.yaml`.

    Example
    -------

    Given the following segment of `config.yaml`,

        config.yaml
          ...
          sinks:
            ...
            example_sink:
              file: output/*_processed.csv
            ...
          ...

    running,

        src = Source("example_source")
        snk = Sink("example_sink")
        snk.build(src)
        snk.drain(src.draw())

    generates `src` and `snk`. Then, `snk` is built, with the '*' in the pattern being
    replaced by the filenames in `src`. Finally, the files drawn from `src`, drained to
    `snk`, and written to files. Any `kwargs` specified will be passed to the internal
    call to `pandas.DataFrame.to_csv`.
    """

    def __init__(self, name: Union[int, str]):
        """
        Data sink for pipelines, built from `config.yaml`.

        Parameters
        ----------

        `name: int, str`: The key that identifies the sink in `config.yaml`.
        """

        self.dfs: List[DataFrame] = []
        self.config = CONFIG["sinks"][name]
        self.file = self.config["file"].as_filename()
        if not in_config_path(self.file):
            raise exc.FileNotInConfigDir(self.file, self.config)
        self.kwargs = self.config["kwargs"].get(cf.Template(default={}))
        self.files: List[str] = []

    def build(self, source: Source = None):
        """
        Patterns the sink based on a source, or else just creates the output file.

        Parameters
        ----------

        `source: Source, default None`: Generate the sink file based on `config.yaml`
        alone by default. If `source` is specified and there is an '*' in the sink
        pattern, then pattern the sink files based on the files in `source`.
        """

        # check if the sink should inherit a file pattern from a source
        if "*" in self.file:
            self.star_sink = True
        else:
            self.star_sink = False

        # get filenames
        if self.star_sink and source is None:
            raise exc.PatternedSinkMissingSource(self.file, self.config)
        elif self.star_sink and source is not None:
            (sink_folder, sink_filename) = path.split(self.file)
            file_suffix = sink_filename.strip("*")
            for file in source.files:
                filename = path.basename(file)
                (file_prefix, _) = path.splitext(filename)
                sink_file = path.join(sink_folder, file_prefix + file_suffix)
                self.files.append(sink_file)
        elif not self.star_sink:
            sink_file = self.file
            self.files.append(sink_file)

    def drain_check(self):
        """
        Checks whether the number of sink drains matches the number of pipes passed.
        """

        num_drains = len(self.files)
        num_pipes = len(self.dfs)
        if num_drains != num_pipes:
            raise exc.DrainPipeMismatch(num_drains, num_pipes)

    def drain(self) -> List[DataFrame]:
        """
        Drains data to sink file(s).
        """

        if not self.files:
            raise exc.SinkNotBuilt
        self.drain_check()

        folderpath = path.dirname(self.files[0])
        if not path.isdir(folderpath):
            makedirs(folderpath)

        for df, file in zip(self.dfs, self.files):
            with open(file, "w", newline="") as f:
                df.to_csv(f, **self.kwargs)

        return self.dfs


class Line:
    """
    Pipeline handler. Builds pipelines from `config.yaml`, holds `Source` and `Sink`
    connections as well, allowing for the full data pipeline from source to sink to be
    run. The `module` parameter points to a module imported in the current workspace.
    Custom functions mentioned in `config.yaml` should be found in `module`.

    Parameters
    ----------

    `name: int, str`: The key that identifies the pipeline in `config.yaml`. `module:
    ModuleType`: Module containing custom dataframe-operating functions.

    Example
    -------

    Given the following segment of `config.yaml`,

        config.yaml
            ...
            pipelines:
              ...
              example_pipeline:
                - type: transform
                  function: add_to_col
                  kwargs:
                    col_name: prices
                    val: 1.5
                  staging:
                    desc: Adds 1.5 to column 'prices'
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
                    desc: Checks whether all prices are over $19.
                ...
            ...

    and the module `custom_functions.py`,

        custom_functions.py

            def add_to_col(df, col_name, val):
                df.loc[:, col_name] = df.loc[:, col_name] + val
                return df

            def high_enough(df, col_name, val):
                return df.loc[:, col_name] > val

    running,

        import custom_functions
        import pdpipewrench as pdpw

        src = pdpw.Source("example_source")
        snk = pdpw.Sink("example_sink")
        line = pdpw.Line("example_pipeline", custom_functions)
        pipeline = line.connect(src, snk)
        print(pipeline)
        (dfs_in, dfs_out) = line.run()

    generates `src`, `snk`, and `line` objects. Then, the `src` and `snk` are connected
    to an internal `pipeline`, which is a `pdpipe.PdPipeLine` object. When this pipeline
    is printed, the following output is displayed:

        A pdpipe pipeline:
        [ 0]  Adds 1.5 to column 'prices'
        [ 1]  Drop columns inventory
        [ 2]  Checks whether all prices are over $19.

    The function of this pipeline is apparent from the descriptions of each stage. Some
    stages have custom descriptions specified in the `desc` key of `config.yaml`. Stages
    of `type: pdpipe` have their descriptions auto-generated from the keyword arguments.

    The command `line.run()` pulls data from `src`, passes it through `pipeline`, and
    drains it to `snk`. The returns `dfs_in` and `dfs_out` show that came in from `src`
    and what went to `snk`. In addition to `line.run()`, the first `n` stages of the
    pipeline can be tested on file `m` from the source with `line.test(m,n)`.
    """

    stage_getters: ClassVar = {
        "transform": get_stage_transform,
        "pdpipe": get_stage_pdpipe,
        "verify_all": get_stage_verify,
        "verify_any": get_stage_verify,
        "check": get_stage_engarde,
    }
    choices: ClassVar = [k for k in stage_getters.keys()]

    def __init__(self, name: Union[int, str], module: ModuleType):
        """
        Pipeline handler. Builds pipelines from `config.yaml`, holds `Source` and `Sink`
        connections as well, allowing for the full data pipeline from source to sink to
        be run. The `module` parameter points to a module imported in the current
        workspace. Custom functions mentioned in `config.yaml` should be found in
        `module`.

        Parameters
        ----------

        `name: int, str`: The key that identifies the pipeline in `config.yaml`.
        `module: ModuleType`: Module containing custom dataframe-operating functions.
        """

        self.built = False
        self.module = module
        self.config = CONFIG["pipelines"][name]
        self.stages: List[pdp.PdPipelineStage] = []
        for i, _ in enumerate(self.config):
            stage_config = self.config[i]
            choice = stage_config["type"].as_choice(self.choices)
            get_stage = self.stage_getters[choice]
            stage = get_stage(self, stage_config)
            self.stages.append(stage)

    def build(self) -> pdp.PdPipeline:
        """
        Builds the pipeline based on stages found in `config.yaml`.
        """

        self.pipeline = pdp.PdPipeline([stage for stage in self.stages])
        self.built = True

        return self.pipeline

    def connect(self, source: Source, sink: Sink) -> List[DataFrame]:
        """
        Builds the pipeline if necessary, then connects a source and sink to the
        pipeline.

        Parameters
        ----------

        `source: Source`: The source to be attached to the pipeline.

        `sink: Sink`: The sink to be attached to the pipeline.
        """

        if not self.built:
            self.build()
        sink.build(source)
        self.source = source
        self.source.draw()
        self.sink = sink

        return self.source.dfs

    def run_one(self, source_idx: int = 0, to_stage: int = None) -> DataFrame:
        """
        Runs the pipeline on just one source file.

        Parameters
        ----------

        `to_stage: int, default None`: Run the entire pipeline by default. Otherwise,
        just run up until the specified stage.

        `source_idx: int, default 0`: Run the pipeline for the first file from the
        source (`index = 0`) by default. Otherwise, run the pipeline on the source file
        specified.
        """

        if to_stage is not None:
            to_stage = max(1, min(to_stage, len(self.stages)))
        else:
            to_stage = len(self.stages)

        assert source_idx < len(self.source.dfs)
        df = self.pipeline[0:to_stage].apply(self.source.dfs[source_idx])

        return df

    def run(self, concat_axis: str = "index") -> List[DataFrame]:
        """
        Runs the pipeline. Draws from the source, sends dataframes through the pipeline,
        and drains the resulting dataframes to the sink files.
        """

        dfs = [self.run_one(source_idx=i) for i in range(0, len(self.source.dfs))]

        if len(self.source.files) > 1 and len(self.sink.files) == 1:
            source_filenames = [path.basename(file) for file in self.source.files]
            self.sink.dfs = [concat(dfs, keys=source_filenames, axis=concat_axis)]
        else:
            self.sink.dfs = dfs

        self.sink.drain()
        return self.sink.dfs
