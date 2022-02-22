"""
To Do:
------
- only handles single-channel streaming t-series. expand usages.

"""
import datetime
import os
from pathlib import Path
from typing import (
    ClassVar,
    List,
    Mapping,
    Optional,
    Tuple,
)
from xml.etree import ElementTree

import numpy as np
from intake.source.base import DataSource, Schema
from numpy.typing import DTypeLike

from ._version import get_version
from .common import *

__all__ = [
    "ThorImageArraySource",
    "ThorImageMetadataSource",
]


class ThorImageMetadataSource(DataSource):
    """

    """

    name: ClassVar[str] = "thorimagemetadata"
    container: ClassVar[str] = "python"
    version: str = get_version()
    partition_access: ClassVar[bool] = True

    def __init__(
        self,
        path: PathLike,
        metadata: Optional[Mapping] = None,
        pattern: Optional[str] = "Experiment.xml",
    ):
        super().__init__(metadata=metadata)
        self._path = os.fspath(path)  # initial path argument.
        self.path = None  # resolved path. set once known.
        self.pattern = pattern

    def read(self) -> Mapping:
        self._load_metadata()
        return ElementTree.parse(self._schema["path"])

    def to_dict(self) -> Mapping:

        doc = self.read()

        # Basic info
        md = {}
        md["software_version"] = doc.find("Software").attrib["version"]
        uTime = int(doc.find("Date").attrib["uTime"])
        md["date"] = str(datetime.datetime.fromtimestamp(uTime))

        # Handle capture mode.
        capture_mode = int(doc.find('CaptureMode').attrib['mode'])
        try:
            capture_mode = {0: "z-series", 1: "t-series"}[capture_mode]
        except KeyError:
            raise ValueError("unknown capture mode: {}".format(capture_mode))
        md["capture_mode"] = capture_mode

        # Handle modality
        modality = doc.find('Modality').attrib['name'].lower()
        if modality not in {"camera", "multiphoton", "confocal"}:
            raise ValueError("unknown modality: {}".format(modality))
        md["modality"] = modality

        if modality == "camera":
            frame = self._parse_camera(doc)
            md["frame"] = frame
        elif modality == "multiphoton":
            frame, PMTs, pockels = self._parse_multiphoton(doc)
            md["frame"] = frame
            md["PMTs"] = PMTs
            md["pockels"] = pockels

        return md

    def _get_partition(self, i):
        """Subclasses should return a container object for this partition
        This function will never be called with an out-of-range value for i.
        """
        return self.read()

    def _get_schema(self) -> Schema:

        if self.path is None:
            if os.path.isdir(self._path):
                if not self.pattern:
                    raise FileNotFoundError(self._path)
                self.path = find_file(
                    self.pattern, root_dir=self._path, absolute=True,
                )
            else:
                self.path = find_file(self._path)
        schema = Schema(
            datashape=None,
            dtype=object,
            shape=None,
            npartitions=1,
            path=self.path,
            extra_metadata={},
        )
        return schema

    def _parse_camera(self, doc: ElementTree) -> Mapping:

        # Handle frame capture info
        node = doc.find('Camera').attrib
        name = node["name"]
        shape = (int(node['height']), int(node['width']))
        dtype = np.dtype("<H")
        size = (float(node['heightUM']), float(node['widthUM']))
        channels = 1
        zoom = 1
        averaging = 1 if node["averageMode"] == "0" else int(node['averageNum'])
        exposure = float(node['exposureTimeMS']) / 1000
        rate = (1 / exposure) / averaging
        binning = (int(node['binningY']), int(node['binningX']))

        frame = dict(
            name=name,
            shape=shape,
            dtype=dtype,
            size=size,
            channels=channels,
            zoom=zoom,
            averaging=averaging,
            rate=rate,
            exposure=exposure,
            binning=binning,
        )
        return frame

    def _parse_multiphoton(
        self,
        doc: ElementTree,
    ) -> Tuple[Mapping, List[Mapping], List[Mapping]]:

        # Handle frame shape, FOV, and pixel size.
        node = doc.find('LSM').attrib
        name = node["name"]
        shape = (int(node['pixelX']), int(node['pixelY']))
        dtype = np.dtype("<H")
        size = (float(node['heightUM']), float(node['widthUM']))
        channels = bin(int(doc.find('Wavelengths/ChannelEnable').get('Set')))[2:].count("1")
        zoom = int(node["pixelY"]) / shape[0]
        averaging = 1 if node["averageMode"] == "0" else int(node['averageNum'])
        rate = float(node['frameRate']) / averaging
        frame = dict(
            name=name,
            shape=shape,
            dtype=dtype,
            size=size,
            channels=channels,
            zoom=zoom,
            averaging=averaging,
            rate=rate,
        )

        # PMTs
        PMTs = []
        pmt_node = doc.find("PMT")
        for index, letter in enumerate("ABCD"):
            enabled = bool(int(pmt_node.attrib[f"enable{letter}"]))
            gain = float(pmt_node.attrib[f"gain{letter}"])
            pmt = dict(enabled=enabled, gain=gain)
            PMTs.append(pmt)

        # Pockels
        pockels = []
        pockels_nodes = doc.findall("Pockels")
        for n in pockels_nodes:
            pock = dict(start=float(n.attrib["start"]), stop=float(n.attrib["stop"]))
            pockels.append(pock)

        return frame, PMTs, pockels


class ThorImageArraySource(DataSource):
    """
    Parameters
    ----------
    path: path-like
        Location of raw image file.
    metadata_path: path-like, optional
        Location of xml metadata file. If not absolute, will look in same
        directory as the raw image file.
    shape: tuple of int, optional
        If not given, will look in metadata file.
    dtype: dtype-like, optional
        If not given, will look in metadata file.
    chunks: int, optional
        Size of chunks within a file along biggest dimension - need not
        be an exact factor of the length of that dimension.
    """

    name: ClassVar[str] = "thorimagearray"
    version: ClassVar[str] = get_version()
    container: ClassVar[str] = "ndarray"
    partition_access: ClassVar[bool] = True

    def __init__(
        self,
        path: PathLike,
        shape: Optional[Tuple[int, ...]] = None,
        dtype: DTypeLike = np.dtype("<H"),
        chunks: Optional[int] = None,
        metadata: Optional[Mapping] = None,
        pattern: str = "Image*.raw",
    ):
        super().__init__(metadata=metadata)

        self._path = path
        self.path = None
        self.shape = shape
        self.dtype = dtype
        self.npartitions = None
        self.chunks = None
        self._chunks_arg = -1 if not chunks else chunks
        self.pattern = pattern

        self._memmap = None
        self._arr = None

    def get_schema(self) -> Schema:
        self._load_metadata()
        return self._schema

    def open(self):
        self._load_metadata()

    def to_dask(self):
        self._load_metadata()
        return self._arr

    def to_memmap(self) -> np.ndarray:
        self._load_metadata()
        return self._memmap

    def read(self) -> np.ndarray:
        self._load_metadata()
        return self._arr.compute()

    def read_partition(self, i: int) -> np.ndarray:
        return self._get_partition(i).compute()

    def _close(self) -> None:
        self._schema = None
        self._memmap = None
        self._arr = None

    def _get_partition(self, i):
        self._load_metadata()
        return self._arr.blocks[i]

    def _get_schema(self) -> Schema:
        """
        Creates and sets the `_arr` attribute -- the raw array accessor.

        Returns
        -------

        """

        import dask.array

        if self._arr is None:

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

            if self.shape is None:
                parent_dir = Path(self.path).parent
                md = ThorImageMetadataSource(parent_dir).to_dict()
                channels = md["frame"]["channels"]
                if channels != 1:
                    raise NotImplementedError('unsupport number of channels')
                frame_shape = md["frame"]["shape"]
                dtype = np.dtype(md["frame"]["dtype"])
                framesize = int(np.prod(frame_shape) * dtype.itemsize)
                filesize = os.stat(self.path).st_size
                self.shape = (filesize // framesize, *frame_shape)
                extra_metadata = md
            else:
                extra_metadata = {}

            self.shape = tuple(self.shape)
            self.dtype = np.dtype(self.dtype)

            if self.chunks is None:
                self.chunks = [-1] * len(self.shape)
                self.chunks[0] = self._chunks_arg

            self._memmap = np.memmap(
                self.path,
                shape=self.shape,
                dtype=self.dtype,
                mode="r",
            )

            self._arr = dask.array.from_array(self._memmap, chunks=self.chunks)
            self.chunks = self._arr.chunks

        return Schema(
            path=self.path,
            shape=self.shape,
            dtype=self.dtype,
            chunks=self.chunks,
            npartitions=1,
            extra_metadata=extra_metadata,
        )

    def _load_metadata(self):
        """load metadata only if needed"""

        if self._schema is None:
            self._schema = self._get_schema()
