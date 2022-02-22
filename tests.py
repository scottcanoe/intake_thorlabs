from pathlib import Path
from intake_thorlabs import *


DATADIR = Path.home() / "test_data"
dirpath1 = DATADIR / "1"
dirpath1 = DATADIR / "2"
dirpath1 = DATADIR / "3"


def test_metadata_camera():

    dirpath = DATADIR / "1"

    path = dirpath / "Experiment.xml"
    src = ThorImageMetadataSource(path)
    doc = src.read()
    md = src.to_dict()

    path = dirpath
    src = ThorImageMetadataSource(path)
    doc = src.read()
    md = src.to_dict()


def test_metadata_multiphoton():

    dirpath = DATADIR / "2"

    path = dirpath / "Experiment.xml"
    src = ThorImageMetadataSource(path)
    doc = src.read()
    md = src.to_dict()

    path = dirpath
    src = ThorImageMetadataSource(path)
    doc = src.read()
    md = src.to_dict()


def test_array_camera():

    dirpath = DATADIR / "1"

    path = dirpath / "Image*.raw"
    src = ThorImageArraySource(path)
    arr = src.to_dask()

    path = dirpath
    src = ThorImageArraySource(path)
    arr = src.to_dask()


def test_array_multiphoton():

    dirpath = DATADIR / "2"

    path = dirpath / "Image*.raw"
    src = ThorImageArraySource(path)
    schema = src.get_schema()
    arr = src.to_dask()

    path = dirpath
    src = ThorImageArraySource(path)
    schema = src.get_schema()
    arr = src.to_dask()


def test_thorsync():

    dirpath = DATADIR / "2"

    path = dirpath / "Episode001.h5"
    src = ThorSyncSource(path)
    schema = src.get_schema()
    df = src.read()

    path = dirpath
    src = ThorSyncSource(path, binary=("FrameOut", "FrameTrigger", "Strobe"))
    schema = src.get_schema()
    df = src.get_schema()



# if __name__ == "__main__":
#
#     DATADIR = Path.home() / "test_data"
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
