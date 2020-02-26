import os

from confuse import LazyConfig
from dotenv import load_dotenv

ENV_KEY = "PDPIPEWRENCHDIR"
CONFIG_FILENAME = "config.yaml"
DUMP_FILENAME = "config_test.yaml"

load_dotenv()
if not os.getenv(ENV_KEY):
    os.environ[ENV_KEY] = os.getcwd()
env_val = os.environ[ENV_KEY]
config_filepath = os.path.join(env_val, CONFIG_FILENAME)
dump_filepath = "test.yaml"

config = LazyConfig("Pdpipewrench")


def load():
    pass
    # path = "path"
    # regex = "results*"
    # files = glob(join(path, regex))
    # for file in files:
    #     df = pd.read_csv(file)
    #     print(df["time"].head())
