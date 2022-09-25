from contextlib import ExitStack, contextmanager


@contextmanager
def contexts(cms):
    with ExitStack() as stack:
        yield [stack.enter_context(cls()) for cls in cms]
