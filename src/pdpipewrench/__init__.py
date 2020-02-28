import os
import warnings
from functools import partial, reduce
from glob import glob
from os import path
from sys import exit
from types import ModuleType
from typing import Callable, ClassVar, Dict, List, Optional, Union

import confuse
import pandas as pd
from confuse import LazyConfig
from dotenv import load_dotenv

# ignore warnings for missing scikit-learn and nltk extras
with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    import pdpipe as pdp

# get config from environment variable or look in cwd by default
ENV_KEY = "PDPIPEWRENCHDIR"
CONFIG_FILENAME = "config.yaml"
load_dotenv()
if not os.getenv(ENV_KEY):
    os.environ[ENV_KEY] = os.getcwd()
config_folderpath = os.environ[ENV_KEY]
config_filepath = os.path.join(config_folderpath, CONFIG_FILENAME)
config = LazyConfig("Pdpipewrench", __name__)

# helper functions


def in_config_path(file) -> bool:
    return config_folderpath.casefold() in file.casefold()


def get_kwargs(config: confuse.ConfigView) -> Dict:
    try:
        kwargs = config["kwargs"].get()
    except confuse.NotFoundError:
        kwargs = {}
    return kwargs


# exceptions

# general


class FileNotInConfigDir(Exception):
    """
    Test.
    """

    def __init__(self, file, config):
        config_name = config["file"].name.replace(".", ": ")
        config_value = config["file"].get()
        msg = (
            f"File '{file}' specified in '{CONFIG_FILENAME}' as "
            f"'{config_name}: {config_value}'\n"
            f"is not in '{config_folderpath}'."
        )
        super().__init__(msg)


class UnhandledIfCondition(Exception):
    """
    Else clause reached due to unhandled if/elif conditional structure.
    """

    def __init__(self):
        msg = "Else clause reached due to unhandled if/elif conditional structure."
        super().__init__(msg)


# sink
class StarSinkMissingSource(Exception):
    """
    A "star sink" has a filename pattern that depends on a sources files. If no source
    is specified when creating a star sink, this error is raised.
    """

    def __init__(self, file, config):
        config_name = config["file"].name.replace(".", ": ")
        config_value = config["file"].get()
        msg = (
            f"Sink with '{config_value}' pattern specified in '{CONFIG_FILENAME}' "
            f"at '{config_name}'\n"
            f"expects <source> in call to 'Sink({config.key}, <source>)'."
        )
        super().__init__(msg)


class DrainPipeMismatch(Exception):
    """
    Number of sink drains don't match number of pipes passed.
    """

    def __init__(self, num_drains, num_pipes):
        msg = f"Sink has {num_drains} drains but got {num_pipes} pipes."
        super().__init__(msg)


# source and sink
class Source:
    """
    Sources data.
    """

    def __init__(self, source_name: Union[int, str]):
        """
        Sources data.
        """
        self.config = config["sources"][source_name]
        self.files = glob(self.config["file"].as_filename())
        test_file = self.files[0]
        if not in_config_path(test_file):
            raise FileNotInConfigDir(test_file, self.config)
        self.kwargs = get_kwargs(self.config)

    def draw(self) -> List[pd.DataFrame]:
        """
        Draws data from source file(s).
        """
        self.dfs: List[pd.DataFrame] = []
        for file in self.files:
            self.dfs.append(pd.read_csv(file, **self.kwargs))
        return self.dfs


class Sink:
    """
    Sinks data.
    """

    def __init__(self, sink_name: Union[int, str], source: Source = None):
        """
        Sinks data.
        """
        self.config = config["sinks"][sink_name]
        self.file = self.config["file"].as_filename()
        test_file = self.file
        if not in_config_path(test_file):
            raise FileNotInConfigDir(test_file, self.config)
        self.kwargs = get_kwargs(self.config)
        self.files: List[str] = []
        self.dfs: List[pd.DataFrame] = []

        # check if the sink should inherit a file pattern from a source
        if "*" in self.file:
            self.star_sink = True
        else:
            self.star_sink = False

        # get filenames
        if self.star_sink and source is None:
            raise StarSinkMissingSource(self.file, self.config)
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
        else:
            raise UnhandledIfCondition

    def drain_check(self, num_pipes: int):
        """
        Checks whether the number of sink drains match the number of pipes passed.
        """
        num_drains = len(self.files)
        if num_drains != num_pipes:
            raise DrainPipeMismatch(num_drains, num_pipes)

    def drain(self, dfs: List[pd.DataFrame] = None):
        """
        Drains data to sink file(s).
        """
        if dfs is not None:
            self.dfs = dfs
        self.drain_check(len(self.dfs))
        (folderpath, _) = path.split(self.files[0])
        if not os.path.isdir(folderpath):
            os.makedirs(folderpath)

        if self.star_sink:
            for df, file in zip(self.dfs, self.files):
                with open(file, "w", newline="") as f:
                    df.to_csv(f, **self.kwargs)
        else:  # there is one drain
            file = self.files[0]
            df = pd.concat(self.dfs)
            with open(file, "w", newline="") as f:
                df.to_csv(f, **self.kwargs)


# wrench


def getfun(module: ModuleType, fun_name: str):
    fun_path = [module]
    fun_path.extend(fun_name.split("."))  # type: ignore
    fun = reduce(getattr, fun_path)  # type: ignore
    return fun


class Wrench:
    """
    Creates pipelines.
    """

    def __init__(self, module: ModuleType):
        self.config = config["stages"]
        self.stages = [s for s in self.config]
        self.pipes: List[pdp.PdPipeline] = []
        for stage in self.stages:
            fun: Union[Callable, None] = None

            if "function" in stage.keys():
                config_fun = stage["function"]
                fun_name = config_fun["name"].get()
                fun = getfun(module, fun_name)
                kwargs = get_kwargs(config_fun)
                fun = partial(fun, **kwargs)  # type: ignore

            if "pdp" in stage.keys():
                config_pipe = stage["pdp"]
                pipe_name = config_pipe["name"].get()
                pipe = getfun(pdp, pipe_name)
                kwargs = get_kwargs(config_pipe)
                if fun is not None:
                    self.pipes.append(pipe(fun, **kwargs))
                else:
                    self.pipes.append(pipe(**kwargs))

        self.pipeline = pdp.PdPipeline(self.pipes)
