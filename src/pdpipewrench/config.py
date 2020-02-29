import warnings
from functools import partial, reduce
from types import ModuleType
from typing import Dict

import confuse as cf
from engarde import checks as engc

from . import CONFIG_FOLDERPATH

# ignore warnings for missing scikit-learn and nltk extras
with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    import pdpipe as pdp


# helper functions
def in_config_path(file) -> bool:
    return CONFIG_FOLDERPATH.casefold() in file.casefold()


def get_or_empty(config: cf.ConfigView) -> Dict:
    try:
        kwargs = config.get()
    except cf.NotFoundError:
        kwargs = {}
    return kwargs


def getfun(fun_name: str, module: ModuleType):
    fun_path = [module]
    fun_path.extend(fun_name.split("."))  # type: ignore
    fun = reduce(getattr, fun_path)  # type: ignore
    return fun


# Stage composition for Pipe class
# TODO: Catch exceptions giving info about errors in YAML file.


def get_stage_transform(pipe) -> pdp.PdPipelineStage:
    config = pipe.config
    module = pipe.parent.module

    # get transform (returns df)
    function_name = config["function"].as_str()
    function = getfun(function_name, module)

    # get args for transform and stage
    kwargs = get_or_empty(config["kwargs"])
    handling = get_or_empty(config["handling"])

    # bake args into transform, compose stage
    function = partial(function, **kwargs)
    stage = pdp.AdHocStage(function, **handling)
    return stage


verify = {"any": "verify_any", "all": "verify_all"}


def get_stage_verify(pipe) -> pdp.PdPipelineStage:
    config = pipe.config
    module = pipe.parent.module

    # get check/transform (returns df[bool])
    check_name = config["function"].as_str()
    check = getfun(check_name, module)

    # get engarde.verify_all or engarde.verify_any
    module = engc
    verify_fun_name = verify[config["cond"].as_str()]
    verify_fun = getfun(verify_fun_name, engc)

    # get args for boolean transform and stage
    kwargs = get_or_empty(config["kwargs"])
    handling = get_or_empty(config["handling"])

    # bake args into engarde.verify-type transform, compose stage
    function = partial(verify_fun, check=check, **kwargs)
    stage = pdp.AdHocStage(function, **handling)
    return stage


def get_stage_pdpipe(pipe) -> pdp.PdPipelineStage:
    config = pipe.config
    module = pdp

    # get a standard pdpipe transform, which is also a stage
    function_name = config["function"].as_str()
    function = getfun(function_name, module)

    # get pdpipe transform **kwargs and **handling kwargs for staging
    kwargs = get_or_empty(config["kwargs"])
    handling = get_or_empty(config["handling"])

    # compose stage
    stage = function(**kwargs, **handling)
    return stage


def get_stage_engarde(pipe) -> pdp.PdPipelineStage:
    config = pipe.config
    module = engc

    # get a standard engarde pipeline check (returns df[bool])
    function_name = config["function"].as_str()
    function = getfun(function_name, module)

    # get args for engarde transform and stage
    kwargs = get_or_empty(config["kwargs"])
    handling = get_or_empty(config["handling"])

    # bake args into engarde transform, compose stage
    function = partial(function, **kwargs)
    stage = pdp.AdHocStage(function, **handling)
    return stage
