import warnings
from types import ModuleType
from typing import ClassVar, List, Union

from . import CONFIG
from . import config as cfg
from .sources import Source, Sink

# ignore warnings for missing scikit-learn and nltk extras
with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    import pdpipe as pdp


class Wrench:
    """
    A wrench.
    """

    def __init__(self, module: ModuleType, name: Union[int, str]):
        """
        A pipeline.
        """
        self.module = module
        self.config = CONFIG["pipelines"][name]
        self.pipes: List[Pipe] = []
        for i, _ in enumerate(self.config):
            self.pipes.append(Pipe(self, i))

    def build(self, source: Source, sink: Sink) -> pdp.PdPipeline:
        # TODO
        print("Method not yet implemented.")
        pipeline = self.build_pipeline()
        self.connect_source(source)
        self.connect_sink(sink)
        return pipeline

    def build_pipeline(self) -> pdp.PdPipeline:
        self.pipeline = pdp.PdPipeline([pipe.stage for pipe in self.pipes])
        return self.pipeline

    def connect_source(self, source: Source):
        # TODO
        print("Method not yet implemented.")

    def connect_sink(self, sink: Sink):
        # TODO
        print("Method not yet implemented.")

    def test_run(self):
        # TODO
        print("Method not yet implemented.")


class Pipe:
    """
    A pipe.
    """

    stage_getters: ClassVar = {
        "transform": cfg.get_stage_transform,
        "verify": cfg.get_stage_verify,
        "pdpipe": cfg.get_stage_pdpipe,
        "engarde": cfg.get_stage_engarde,
    }
    choices = [k for k in stage_getters.keys()]

    def __init__(self, wrench: Wrench, pipe_number: int):
        """
        A pipe.
        """
        self.parent = wrench
        self.config = wrench.config[pipe_number]
        choice = self.config["type"].as_choice(self.choices)
        get_stage = self.stage_getters[choice]
        self.stage = get_stage(self)
