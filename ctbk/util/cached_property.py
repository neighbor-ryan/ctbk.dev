class cached_property:
    """Based on code from David Beazley's "Python Cookbook" / https://stackoverflow.com/q/62160411/544236."""
    def __init__(self, func):
        self.__doc__ = getattr(func, '__doc__')
        self.func = func

    def __get__(self, instance, cls):
        if instance is None:
            return self
        else:
            value = instance.__dict__[self.func.__name__] = self.func(instance)
            return value
