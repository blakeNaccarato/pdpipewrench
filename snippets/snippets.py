# flake8: noqa
# pylint: disable-all
# type: ignore

#? Not yet implemented.
functions: List[Callable] = []
for stage in config["stages"]:
    function = getattr(__name__, stage["function"].get())
    args = stage["args"].get()
    function = partial(function, **args)
    functions.append(function)

#? Originally in pdpipewrench/__init__.py as a check on source and sink filenames.
#? Removed after implementing source/sink classes.
# get paths from config.yaml, either relative to config.yaml dir or absolute
data_template = {"data": {"source": confuse.Filename(), "sink": confuse.Filename()}}
# if key is missing and not in pdpipewrench/config_default.yaml, raise a descriptive
# error based on the original, less descriptive confuse.NotFoundError
try:
    data_files = config.get(data_template)
except confuse.NotFoundError as exc:
    msg = exc.args[0]
    missing_key = msg.split()[0].replace(".", ": ")
    msg = f"Missing <value> for '{missing_key}: <value>' in \n{config_filepath}"
    raise confuse.NotFoundError(msg)
