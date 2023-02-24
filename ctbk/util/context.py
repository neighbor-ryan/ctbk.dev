import shutil

from contextlib import contextmanager


def contexts(ctxs):
    @contextmanager
    def fn(ctxs):
        if not ctxs:
            yield
        else:
            [ ctx, *rest ] = ctxs
            with ctx, fn(rest):
                yield
    return fn(ctxs)


@contextmanager
def copy_ctx(src, dst, src_fs=None, src_mode='rb', dst_fs=None, dst_mode='wb'):
    src_ctx = src_fs.open(src, src_mode) if src_fs else open(src, src_mode)
    dst_ctx = dst_fs.open(dst, dst_mode) if dst_fs else open(dst, dst_mode)
    with src_ctx as src_fd, dst_ctx as dst_fd:
        yield
        print(f'copying: {src} to {dst}')
        shutil.copyfileobj(src_fd, dst_fd)
        print('copied')
