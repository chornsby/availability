class VariableBoolean:
    """An object whose boolean value changes each time you evaluate it.

    This is useful when mocking module level constants in tests to avoid infinte
    loops.

    >>> is_enabled = VariableBoolean(True, True, False)
    >>> bool(is_enabled)
    True
    >>> bool(is_enabled)
    True
    >>> bool(is_enabled)
    False
    >>> bool(is_enabled)
    Traceback (most recent call last):
        ...
    ValueError: Called more times than has values
    """

    def __init__(self, *values: bool):
        self._values = list(values)

    def __bool__(self):
        try:
            return self._values.pop(0)
        except IndexError as error:
            raise ValueError("Called more times than has values") from error
