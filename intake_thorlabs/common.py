import glob
import os
from os import PathLike
from pathlib import Path
from typing import List, Optional

from numpy.typing import ArrayLike, DTypeLike

__all__ = [
    "ArrayLike",
    "DTypeLike",
    "find_file",
    "find_files",
    "PathLike",
]


def find_file(
    pathname: PathLike,
    *,
    root_dir: Optional[PathLike] = None,
    recursive: bool = False,
    absolute: bool = False,
) -> str:
    """
    Return the unique path specified by `pathname` or raise `FileNotFoundError`.

    Parameters
    ----------
    pathname: path-like
        File/pattern to search for. If it is relative, `root_dir` must be
        specified and be absolute.
    root_dir: path-like, optional
        If not `None`, it  should be an absolute path-like object specifying
        the root directory for searching.
    recursive: bool, optional
        If `True`, the pattern “**” will match any files and zero or more
        directories, subdirectories and symbolic links to directories.
        If the pattern is followed by an `os.sep` or `os.altsep` then files
        will not match.
    absolute: bool, optional
        If `True`, always returns absolute paths. This overrides the default
        behavior in which relative paths are returned if `root_dir` was
        specified.

    Returns
    -------
    path: str
        Unique path matching the given pattern. If `root_dir` was specified,
        paths will be relative to `root_dir` unless `absolute` is `True`.

    Raises
    ------
    `ValueError`:
        Raised if pattern is not absolute after optionally accounting
        for `root_dir`.
    `FileNotFoundError`:
        Raised if 0 or >1 files found.
    """

    matches = find_files(
        pathname,
        root_dir=root_dir,
        recursive=recursive,
        absolute=absolute,
    )
    if len(matches) != 1:
        msg = f"{len(matches)} files found for pathname {pathname} with " \
              f"root_dir {root_dir}, recursive={recursive}"
        raise FileNotFoundError(msg)
    return matches[0]


def find_files(
    pathname: PathLike,
    *,
    root_dir: Optional[PathLike] = None,
    recursive: bool = False,
    absolute: bool = False,
) -> List[str]:
    """
    Return a list of files that match a given pattern.


    Parameters
    ----------
    pathname: path-like
        File/pattern to search for. If it is relative, `root_dir` must be
        specified and be absolute.
    root_dir: path-like, optional
        If not `None`, it  should be an absolute path-like object specifying
        the root directory for searching.
    recursive: bool, optional
        If `True`, the pattern “**” will match any files and zero or more
        directories, subdirectories and symbolic links to directories.
        If the pattern is followed by an `os.sep` or `os.altsep` then files
        will not match.
    absolute: bool, optional
        If `True`, always returns absolute paths. This overrides the default
        behavior in which relative paths are returned if `root_dir` was
        specified.

    Returns
    -------
    paths: list[str]
        Zero or more paths matching the given pattern. If `root_dir` was
        specified, paths will be relative to `root_dir` unless `absolute`
        is `True`.

    Raises
    ------
    `ValueError`:
        Raised if pattern is not absolute after optionally accounting
        for `root_dir`.
    """

    path = Path(os.path.expanduser(pathname))
    root = Path(os.path.expanduser(root_dir)) if root_dir else None
    pattern = root / path if root else path
    if not pattern.is_absolute():
        raise ValueError("path must be absolute if root_dir not specified. "
                         "Otherwise root_dir must be absolute"
                         )

    # handle wildcard/glob patterns
    matches = glob.glob(str(pattern), recursive=recursive)
    if root and root.is_absolute() and not absolute:
        matches = [os.path.relpath(p, root) for p in matches]
    return matches
