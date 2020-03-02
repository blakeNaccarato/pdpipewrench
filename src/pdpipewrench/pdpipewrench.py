"""
Module docstring.
"""

import warnings
from functools import partial, reduce
from glob import glob
from os import makedirs, path
from types import ModuleType
from typing import ClassVar, List, Union, Tuple

import confuse as cf
from engarde import checks as engc
from pandas import DataFrame, concat, read_csv

from . import CONFIG, CONFIG_FOLDERPATH
from . import exceptions as exc

# ignore warnings for missing scikit-learn and nltk extras
with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    import pdpipe as pdp


# * HELPER FUNCTIONS * #


def in_config_path(file: str) -> bool:
    """
    Checks whether a file is in the config directory or one of its subdirectories.
    """

    return CONFIG_FOLDERPATH.casefold() in file.casefold()


def getfun(fun_name: str, module: ModuleType):
    """
    Finds a function through successive application of `getattr`.
    """

    fun_path = [module]
    fun_path.extend(fun_name.split("."))  # type: ignore
    fun = reduce(getattr, fun_path)  # type: ignore
    return fun


# * STAGE COMPOSITION * #
# type.
#   transform. custom function wrapped in pdp.AdHocStage
#       function. name of custom function
#       kwargs. kwargs passed to function
#       staging
#   pdpipe. builtin pdp
#       function. name of pdp builtin stage
#       kwargs. kwargs passed to pdp builtin stage
#       staging
#   verify-all/verify-any. custom check wrapped in engc.verify-all/engc.verify-any
#       check. name of custom check.
#       kwargs. kwargs passed to check.
#       staging
#   check. builtin engc
#       check. name of engc builtin check.
#       kwargs. kwargs passed to engc builtin check.
#       staging
#
#   staging.
#       desc. description of stage
#       exraise. true/false. default true. whether to raise exceptions.
#       exmsg. message to print when stage raises an exception.
# TODO: Catch exceptions giving info about errors in YAML file.


def get_stage_transform(line, stage_config: cf.ConfigView) -> pdp.PdPipelineStage:
    module = line.module

    # get transform (returns df)
    function_name = stage_config["function"].as_str()
    function = getfun(function_name, module)

    # get args for transform and stage
    kwargs = stage_config["kwargs"].get(cf.Template(default={}))
    staging = stage_config["staging"].get(cf.Template(default={}))

    # bake args into transform, compose stage
    function = partial(function, **kwargs)
    stage = pdp.AdHocStage(function, **staging)
    return stage


def get_stage_pdpipe(line, stage_config: cf.ConfigView) -> pdp.PdPipelineStage:
    module = pdp

    # get a standard pdpipe transform, which is also a stage
    function_name = stage_config["function"].as_str()
    function = getfun(function_name, module)

    # get pdpipe transform **kwargs and **staging kwargs for staging
    kwargs = stage_config["kwargs"].get(cf.Template(default={}))
    staging = stage_config["staging"].get(cf.Template(default={}))

    # compose stage
    stage = function(**kwargs, **staging)
    return stage


def get_stage_verify(line, stage_config: cf.ConfigView) -> pdp.PdPipelineStage:
    module = line.module

    # get check/transform (returns df[bool])
    check_name = stage_config["check"].as_str()
    check = getfun(check_name, module)

    # get engc.verify_all or engc.verify_any
    module = engc
    verify_fun_name = stage_config["type"].as_str()
    verify_fun = getfun(verify_fun_name, engc)

    # get args for boolean transform and stage
    kwargs = stage_config["kwargs"].get(cf.Template(default={}))
    staging = stage_config["staging"].get(cf.Template(default={}))

    # bake args into engarde.verify-type transform, compose stage
    function = partial(verify_fun, check=check, **kwargs)
    stage = pdp.AdHocStage(function, **staging)
    return stage


def get_stage_engarde(line, stage_config: cf.ConfigView) -> pdp.PdPipelineStage:
    module = engc

    # get a standard engarde pipeline check (returns df[bool])
    check_name = stage_config["check"].as_str()
    check = getfun(check_name, module)

    # get args for engarde check and stage
    kwargs = stage_config["kwargs"].get(cf.Template(default={}))
    staging = stage_config["staging"].get(cf.Template(default={}))

    # bake args into engarde transform, compose stage
    function = partial(check, **kwargs)
    stage = pdp.AdHocStage(function, **staging)
    return stage


# * CLASSES * #


class Source:
    """
    Sources data.
    """

    def __init__(self, source_name: Union[int, str]):
        """
        Sources data.
        """
        self.config = CONFIG["sources"][source_name]
        self.files = glob(self.config["file"].as_filename())
        test_file = self.files[0]
        if not in_config_path(test_file):
            raise exc.FileNotInConfigDir(test_file, self.config)
        self.kwargs = self.config["kwargs"].get(cf.Template(default={}))

    def draw(self, index: int = None) -> List[DataFrame]:
        """
        Draws data from source file(s).
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
    Sinks data.
    """

    def __init__(self, sink_name: Union[int, str]):
        """
        Sinks data.
        """
        self.dfs: List[DataFrame] = []
        self.config = CONFIG["sinks"][sink_name]
        self.file = self.config["file"].as_filename()
        if not in_config_path(self.file):
            raise exc.FileNotInConfigDir(self.file, self.config)
        self.kwargs = self.config["kwargs"].get(cf.Template(default={}))
        self.files: List[str] = []

    def build(self, source: Source = None):
        """
        Patterns the sink based on a source, or else just creates the output file.
        """

        # check if the sink should inherit a file pattern from a source
        if "*" in self.file:
            self.star_sink = True
        else:
            self.star_sink = False

        # get filenames
        if self.star_sink and source is None:
            raise exc.StarSinkMissingSource(self.file, self.config)
        elif self.star_sink and source is not None:
            (sink_folder, sink_filename) = path.split(self.file)
            file_suffix = sink_filename.strip("*")
            for file in source.files:
                (_, filename) = path.split(file)
                (file_prefix, _) = path.splitext(filename)
                sink_file = path.join(sink_folder, file_prefix + file_suffix)
                self.files.append(sink_file)
        elif not self.star_sink:
            sink_file = self.file
            self.files.append(sink_file)

    def drain_check(self):
        """
        Checks whether the number of sink drains match the number of pipes passed.
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

        (folderpath, _) = path.split(self.files[0])
        if not path.isdir(folderpath):
            makedirs(folderpath)

        if self.star_sink:
            for df, file in zip(self.dfs, self.files):
                with open(file, "w", newline="") as f:
                    df.to_csv(f, **self.kwargs)
        else:  # there is one drain
            file = self.files[0]
            df = concat(self.dfs)
            with open(file, "w", newline="") as f:
                df.to_csv(f, **self.kwargs)

        return self.dfs


class Line:
    """
    A line.
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
        A pipeline.
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
        self.pipeline = pdp.PdPipeline([stage for stage in self.stages])
        self.built = True
        return self.pipeline

    def connect(self, source: Source, sink: Sink) -> pdp.PdPipeline:
        """
        Connects a source and sink to the pipeline.
        """
        if not self.built:
            self.build()
        sink.build(source)
        self.source = source
        self.sink = sink
        return self.pipeline

    def run(self, up_to: int = None) -> Tuple[List[DataFrame], List[DataFrame]]:
        """
        Runs the pipeline. Draws from the source, sends dataframes through the pipeline,
        and drains the resulting dataframes to the sink files.
        """

        if up_to is not None:
            up_to = max(1, min(up_to, len(self.pipeline)))
        else:
            up_to = len(self.pipeline)

        self.source.draw()
        self.sink.dfs = [self.pipeline[0:up_to](df) for df in self.source.dfs]
        if len(self.source.files) > 1 and len(self.sink.files) == 1:
            pass
        self.sink.drain()
        return self.source.dfs, self.sink.dfs

    def test(self, up_to: int = None, index: int = 0) -> DataFrame:
        """
        Tests the pipeline on just one file from the source.
        """

        if up_to is not None:
            up_to = max(1, min(up_to, len(self.pipeline)))
        else:
            up_to = len(self.pipeline)

        self.source.draw(0)
        test_df = self.pipeline[0:up_to](self.source.dfs[index])
        return test_df
