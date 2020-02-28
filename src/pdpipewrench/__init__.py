import os

from confuse import LazyConfig
from dotenv import load_dotenv

from .sources import Source, Sink  # noqa: F401
# from .forge import Pipeline, Pipe

# get config from environment variable or look in cwd by default
ENV_KEY = "PDPIPEWRENCHDIR"
CONFIG_FILENAME = "config.yaml"
load_dotenv()
if not os.getenv(ENV_KEY):
    os.environ[ENV_KEY] = os.getcwd()
CONFIG_FOLDERPATH = os.environ[ENV_KEY]
CONFIG = LazyConfig("Pdpipewrench", __name__)
