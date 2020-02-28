from glob import glob
from os import makedirs, path
from typing import List, Union

from pandas import DataFrame, concat, read_csv

from . import config as cfg
from . import exceptions as exc
from pdpipewrench import CONFIG


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
        if not cfg.in_config_path(test_file):
            raise exc.FileNotInConfigDir(test_file, self.config)
        self.kwargs = cfg.get_kwargs(self.config)

    def draw(self) -> List[DataFrame]:
        """
        Draws data from source file(s).
        """
        self.dfs: List[DataFrame] = []
        for file in self.files:
            self.dfs.append(read_csv(file, **self.kwargs))
        return self.dfs


class Sink:
    """
    Sinks data.
    """

    def __init__(self, sink_name: Union[int, str], source: Source = None):
        """
        Sinks data.
        """
        self.config = CONFIG["sinks"][sink_name]
        self.file = self.config["file"].as_filename()
        test_file = self.file
        if not cfg.in_config_path(test_file):
            raise exc.FileNotInConfigDir(test_file, self.config)
        self.kwargs = cfg.get_kwargs(self.config)
        self.files: List[str] = []
        self.dfs: List[DataFrame] = []

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
        else:
            raise exc.UnhandledIfCondition

    def drain_check(self, num_pipes: int):
        """
        Checks whether the number of sink drains match the number of pipes passed.
        """
        num_drains = len(self.files)
        if num_drains != num_pipes:
            raise exc.DrainPipeMismatch(num_drains, num_pipes)

    def drain(self, dfs: List[DataFrame] = None):
        """
        Drains data to sink file(s).
        """
        if dfs is not None:
            self.dfs = dfs
        self.drain_check(len(self.dfs))
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
