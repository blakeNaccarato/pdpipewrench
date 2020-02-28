# ignore warnings for missing scikit-learn and nltk extras
import warnings
from functools import partial, reduce
from types import ModuleType
from typing import Callable, List, Union

from . import CONFIG
from . import config as cfg

with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    import pdpipe as pdp


def getfun(module: ModuleType, fun_name: str):
    fun_path = [module]
    fun_path.extend(fun_name.split("."))  # type: ignore
    fun = reduce(getattr, fun_path)  # type: ignore
    return fun


class Pipeline:
    """
    A pipeline.
    """

    def __init__(self, pipeline_name: Union[int, str]):
        pass


class Pipe:
    """
    A pipe.
    """

    def __init__(self, pipeline_name: str, pipe_number: int):
        self.config = CONFIG[pipe_number]


class Forge:
    """
    Creates pipelines.
    """

    def __init__(self, module: ModuleType):
        self.config = CONFIG["stages"]
        self.stages = [s for s in self.config]
        self.pipes: List[pdp.PdPipeline] = []
        for stage in self.stages:
            fun: Union[Callable, None] = None

            if "function" in stage.keys():
                config_fun = stage["function"]
                fun_name = config_fun["name"].get()
                fun = getfun(module, fun_name)
                kwargs = cfg.get_kwargs(config_fun)
                fun = partial(fun, **kwargs)  # type: ignore

            if "pdp" in stage.keys():
                config_pipe = stage["pdp"]
                pipe_name = config_pipe["name"].get()
                pipe = getfun(pdp, pipe_name)
                kwargs = cfg.get_kwargs(config_pipe)
                if fun is not None:
                    self.pipes.append(pipe(fun, **kwargs))
                else:
                    self.pipes.append(pipe(**kwargs))

        self.pipeline = pdp.PdPipeline(self.pipes)
