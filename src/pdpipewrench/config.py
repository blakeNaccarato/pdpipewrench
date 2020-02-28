from typing import Dict

import confuse

from . import CONFIG_FOLDERPATH


def in_config_path(file) -> bool:
    return CONFIG_FOLDERPATH.casefold() in file.casefold()


def get_kwargs(config: confuse.ConfigView) -> Dict:
    try:
        kwargs = config["kwargs"].get()
    except confuse.NotFoundError:
        kwargs = {}
    return kwargs
