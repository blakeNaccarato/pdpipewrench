from . import CONFIG_FILENAME, CONFIG_FOLDERPATH


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
            f"is not in '{CONFIG_FOLDERPATH}'."
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
