import os
from numbers import Number
from typing import (
    ClassVar,
    Container,
    Mapping,
    Optional,
    Set,
)

import fsspec
import h5py
import numpy as np
import pandas as pd
from intake.source.base import DataSource, Schema

from .common import *

__all__ = [
    "ThorSyncSource",
]


class ThorSyncSource(DataSource):
    """
    Driver for ThorSync's h5 output.

    Parameters
    ----------
    path: path-like
    Path to h5 file. Usually called 'Episode001.h5'.

    binary: iterable of str
    Digital lines carrying binary data. Values are squashed into {0, 1} and the dtype is cast to np.int8.



    """

    name: ClassVar[str] = "thorsync"
    container: ClassVar[str] = "dataframe"
    version: str = "0.0.1"
    partition_access: ClassVar[bool] = False

    def __init__(
        self,
        path: PathLike,
        *,
        binary: Optional[Container[str]] = None,
        clock_rate: Number = 20_000_000,
        pattern: str = "Episode*.h5",
        metadata: Optional[Mapping] = None,
    ):
        super().__init__(metadata=metadata)
        self._path = path
        self.path = None
        self.binary = binary
        self.clock_rate = clock_rate
        self.pattern = pattern
        self._dataframe = None

    @property
    def binary(self) -> Set:
        return self._binary

    @binary.setter
    def binary(self, val: Optional[Container[str]]) -> None:
        self._binary = set(val) if val else set()

    def get_schema(self) -> Schema:
        self._load_metadata()
        return self._get_schema()

    def _close(self) -> None:
        self._schema = None
        self._dataframe = None

    def _get_partition(self, i):
        """Subclasses should return a container object for this partition
        This function will never be called with an out-of-range value for i.
        """
        if self._dataframe is None:
            self._dataframe = self._load_dataframe()
        return self._dataframe

    def _get_schema(self) -> Schema:

        if not self.path or not os.path.exists(self.path):
            # locate raw data file
            if os.path.isdir(self._path):
                if not self.pattern:
                    raise FileNotFoundError(self._path)
                self.path = find_file(
                    self.pattern, root_dir=self._path, absolute=True,
                )
            else:
                self.path = find_file(self._path)

        with fsspec.open_files(self.path, "rb")[0] as file:
            with h5py.File(file, "r") as f:
                clock = f['Global']['GCtr']
                length = clock.shape[0]
                dtypes = {"time": np.float64}

                AI = f["AI"]
                for name, dset in AI.items():
                    dtypes[name] = dset.dtype

                DI = f["DI"]
                for name, dset in DI.items():
                    if name in self.binary:
                        dtypes[name] = np.int8

        shape = (length, len(dtypes))
        columns = tuple(dtypes.keys())

        return Schema(
            dtype=None,
            shape=shape,
            npartitions=1,
            path=self.path,
            columns=columns,
            dtypes=dtypes,
            extra_metadata={},
        )

    def _load_metadata(self) -> None:
        if self._schema is None:
            self._schema = self._get_schema()

    def _load_dataframe(self) -> pd.DataFrame:
        """
        Load the h5 data into a dataframe and return it.
        """
        self._load_metadata()

        file = fsspec.open_files(self.path, "rb")[0]
        with file as f_inner:
            with h5py.File(f_inner, "r") as f:
                data = {}
                # Create time array from 20 kHz clock ticks. Thorsync's
                # metadata file has samplerate entries, but Thorlabs'
                # house-made matlab scripts have this value hard-corded in.
                # Also, this value isn't one of the samplerates listed in
                # the metadata file ('ThorRealTimeDataSettings.xml').
                clock_rate = self.clock_rate
                clock = f["Global"]["GCtr"][:].reshape(-1)
                data['time'] = clock / clock_rate

                # Load analog lines.
                for name, dset in f['AI'].items():
                    arr = dset[:].reshape(-1)
                    data[name] = arr

                # Load digital lines.
                for name, dset in f['DI'].items():
                    arr = dset[:].reshape(-1)
                    if name in self._binary:
                        # For some reason, some digital lines that should
                        # carry only 0s or 1s have 0s and 2s or 0s and 16s.
                        # Clip them here.
                        arr = np.clip(arr, 0, 1).astype(np.int8)
                    else:
                        # Prefer signed integers to avoid pitfalls with diff.
                        arr = arr.astype(np.int32)
                    data[name] = arr

        df = pd.DataFrame(data)

        return df

