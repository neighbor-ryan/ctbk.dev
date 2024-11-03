import struct
from gzip import GzipFile


class DeterministicGzipFile(GzipFile):
    """
    A GzipFile subclass that writes deterministic output by:
    1. Setting fixed metadata values in the header
    2. Using a consistent modification time
    3. Not including the filename in the header
    """
    def _write_gzip_header(self, compresslevel):
        self.fileobj.write(b'\037\213')             # Magic number
        self.fileobj.write(b'\010')                 # Compression method
        fname = b''
        flags = 0
        self.fileobj.write(chr(flags).encode('latin-1'))
        mtime = 0  # Using 0 as a fixed modification time
        self.fileobj.write(struct.pack("<L", mtime))
        # Set extra flags based on compression level:
        # 2 -> maximum compression
        # 4 -> fastest algorithm
        xfl = 2 if compresslevel >= 9 else (4 if compresslevel == 1 else 0)
        self.fileobj.write(bytes([xfl]))
        self.fileobj.write(b'\377')                 # OS (unknown)
        if fname:
            self.fileobj.write(fname + b'\000')
