import io
from os import PathLike
from pathlib import Path
from types import SimpleNamespace
from typing import Mapping, Tuple, Union
from unittest import TestCase
import xml
from xml.etree import ElementTree

import numpy as np
import pandas as pd
from intake_thorlabs import *

DATADIR = Path.home() / "test_data"
dirpath1 = DATADIR / "1"
dirpath2 = DATADIR / "2"
dirpath3 = DATADIR / "3"


class TestThorImageMetadata(TestCase):


    def test_camera(self):

        all_paths = [
            DATADIR / "camera/Experiment.xml",
            DATADIR / "camera",
        ]
        results = []
        for path in all_paths:
            src = ThorImageMetadataSource(path)
            tree, buf = src.read(), io.BytesIO()
            tree.write(buf)
            bts = buf.getvalue()
            dct = src.to_dict()
            ns = SimpleNamespace(path=path, src=src, bts=bts, dct=dct)
            results.append(ns)

        a, b = results
        self.assertEqual(a.src.path, b.src.path)
        self.assertEqual(a.bts, b.bts)
        self.assertEqual(a.dct, b.dct)


    def test_multiphoton(self):

        all_paths = [
            DATADIR / "multiphoton/Experiment.xml",
            DATADIR / "multiphoton",
        ]
        results = []
        for path in all_paths:
            src = ThorImageMetadataSource(path)
            tree, buf = src.read(), io.BytesIO()
            tree.write(buf)
            bts = buf.getvalue()
            dct = src.to_dict()
            ns = SimpleNamespace(path=path, src=src, bts=bts, dct=dct)
            results.append(ns)

        a, b = results
        self.assertEqual(a.src.path, b.src.path)
        self.assertEqual(a.bts, b.bts)
        self.assertEqual(a.dct, b.dct)


class TestThorImageArray(TestCase):


    def test_camera(self):

        all_paths = [
            DATADIR / "camera",
            DATADIR / "camera",
        ]
        results = []
        for path in all_paths:
            src = ThorImageArraySource(path)
            arr = src.to_dask()
            ns = SimpleNamespace(src=src, arr=arr)
            results.append(ns)

        a, b = results
        self.assertEqual(a.src.path, b.src.path)
        self.assertTrue(a.arr.shape == b.arr.shape)
        self.assertTrue(a.arr.dtype == b.arr.dtype)
        data1, data2 = a.arr[:50].compute(), b.arr[:50].compute()
        self.assertTrue(np.array_equal(data1, data2))



    def test_multiphoton(self):

        all_paths = [
            DATADIR / "multiphoton/Image*.raw",
            DATADIR / "multiphoton",
        ]
        results = []
        for path in all_paths:
            src = ThorImageArraySource(path)
            arr = src.to_dask()
            ns = SimpleNamespace(src=src, arr=arr)
            results.append(ns)

        a, b = results
        self.assertEqual(a.src.path, b.src.path)
        self.assertTrue(a.arr.shape == b.arr.shape)
        self.assertTrue(a.arr.dtype == b.arr.dtype)
        data1, data2 = a.arr[:50].compute(), b.arr[:50].compute()
        self.assertTrue(np.array_equal(data1, data2))


class TestThorSync(TestCase):


    def test_thorsync(self):

        all_paths = [
            DATADIR / "camera/Episode*.h5",
            DATADIR / "camera",
        ]
        results = []
        for path in all_paths:
            src = ThorSyncSource(path)
            schema = src.get_schema()
            ns = SimpleNamespace(path=path, src=src, schema=schema)
            results.append(ns)

        a, b = results
        self.assertEqual(a.src.path, b.src.path)
        df1, df2 = a.src.read(), b.src.read()
        self.assertTrue(df1.equals(df2))


def test_thorsync(dirpath: Path):
    path = dirpath / "Episode001.h5"
    src = ThorSyncSource(path)
    schema = src.get_schema()
    df = src.read()

    path = dirpath
    src = ThorSyncSource(path, binary=("FrameOut", "FrameTrigger", "Strobe"))
    schema = src.get_schema()
    df = src.get_schema()


if __name__ == "__main__":
    path = DATADIR / "1/Experiment.xml"
    src = ThorImageMetadataSource(path)
    tree = src.read()
    string = ElementTree.tostring(tree)
    md = src.to_dict()

    # unittest.main()

    # DATADIR = Path.home() / "test_data"
    # test_metadata_camera(DATADIR / "1")
    # test_array_camera(DATADIR / "1")
    # test_thorsync(DATADIR / "1")
    #
    # test_metadata_multiphoton(DATADIR / "2")
    # test_array_multiphoton(DATADIR / "2")
    # test_thorsync(DATADIR / "2")

    # make smaller sample datasets?
    # src = ThorImageArraySource(DATADIR / "multiphoton/Image*.raw")
    # arr = src.read()
    # mat = arr[:10]
    #
    # dirpath = DATADIR / "1"
    # path = dirpath / "Experiment.xml"
    # src = ThorImageMetadataSource(path)
    # doc = src.read()
    # md = src.to_dict()
