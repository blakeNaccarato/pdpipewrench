"""
Exceptions found here generally cover data flow issues due to misconfigured
sources/sinks. Additionally, some of these exceptions shadow more cryptic exceptions
raised by package dependencies, and point to issues in the user config.yaml keys and
values.
"""

from . import CONFIG_FILENAME, CONFIG_FOLDERPATH


class FileNotInConfigDir(Exception):
    """
    The path to the configuration file is not contained in the path to the desired
    input/output file(s).
    """

    def __init__(self, file, config):
        config_name = config["file"].name.replace(".", ": ")
        config_value = config["file"].get()
        msg = (
            f"File '{file}' specified in '{CONFIG_FILENAME}' as "
            f"'{config_name}: {config_value}'\n"
            f"is not in '{CONFIG_FOLDERPATH}'."
        )
        super().__init__(msg)


# * ------------------------------------------------------------------------------ * #
# * Sink Exceptions

class PatternedSinkMissingSource(Exception):
    """
    A sink is patterned if there is an asterisk in its "file" configuration key. Such a
    sink must be passed a source when it is built, to inform the pattern that will
    replace the asterisk. If a source is not passed, then this exception is raised.
    """

    def __init__(self, file, config):
        config_name = config["file"].name.replace(".", ": ")
        config_value = config["file"].get()
        msg = (
            f"Sink with '{config_value}' pattern specified in '{CONFIG_FILENAME}' "
            f"at '{config_name}'\n"
            f"expects <source> in call to 'Sink.build(<source>)'."
        )
        super().__init__(msg)


class SinkNotBuilt(Exception):
    """
    An attempt has been made to drain a sink before building it.
    """

    def __init__(self):
        msg = f"Sink is not yet built."
        super().__init__(msg)


class DrainPipeMismatch(Exception):
    """
    The number of sink drains doesn't match the number of pipes passed.
    """

    def __init__(self, num_drains, num_pipes):
        msg = f"Sink has {num_drains} drains but got {num_pipes} pipes."
        super().__init__(msg)
