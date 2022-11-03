"""
We mimic POSIX filesystem API for the distributed storage system. However, we discard unsuitable abstactions for distributed systems such as directories and file permission. Paths can contain hierarchical structure like '/' to organize related files, but they are simply treated as keys for files.
"""


def open(path: str, O_CREAT: bool = False) -> int:
    """
    open a new file and return the fd.
    O_CREAT: If set to True, then create the file on the fly.
    """
