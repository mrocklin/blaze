import os

import numpy as np
import tables as tb

from toolz import first
from .dispatch import dispatch

import datashape

import shutil
from blaze.utils import tmpfile

__all__ = ['PyTables']


def dtype_to_pytables(dtype):
    """ Convert NumPy dtype to PyTable descriptor

    >>> dt = np.dtype([('name', 'S7'), ('amount', 'i4'), ('time', 'M8[us]')])
    >>> dtype_to_pytables(dt)
    {'amount': Int32Col(shape=(), dflt=0, pos=1), 'name': StringCol(itemsize=7, shape=(), dflt='', pos=0), 'time': Time64Col(shape=(), dflt=0.0, pos=2)}
    """
    d = {}
    for pos, name in enumerate(dtype.names):
        dt, _ = dtype.fields[name]
        if issubclass(dt.type, np.datetime64):
            tdtype = tb.Description({name: tb.Time64Col(pos=pos)}),
        else:
            tdtype = tb.descr_from_dtype(np.dtype([(name, dt)]))
        el = first(tdtype)
        getattr(el, name)._v_pos = pos
        d.update(el._v_colobjects)
    return d


def PyTables(path, datapath, dshape=None):
    def possibly_create_table(filename, dtype):
        f = tb.open_file(filename, mode='a')
        try:
            if datapath not in f:
                if dtype is None:
                    raise ValueError('dshape cannot be None and datapath not'
                                     ' in file')
                else:
                    f.create_table('/', datapath.lstrip('/'), description=dtype)
        finally:
            f.close()

    if dshape:
        if isinstance(dshape, str):
            dshape = datashape.dshape(dshape)
        if dshape[0] == datashape.var:
            dshape = dshape.subshape[0]
        dtype = dtype_to_pytables(datashape.to_numpy_dtype(dshape))
    else:
        dtype = None

    if os.path.exists(path):
        possibly_create_table(path, dtype)
    else:
        with tmpfile('.h5') as filename:
            possibly_create_table(filename, dtype)
            shutil.copyfile(filename, path)
    return tb.open_file(path, mode='a').get_node(datapath)


@dispatch(tb.Table)
def chunks(b, chunksize=2**15):
    start = 0
    n = len(b)
    while start < n:
        yield b[start:start + chunksize]
        start += chunksize


@dispatch(tb.Table, int)
def get_chunk(b, i, chunksize=2**15):
    start = chunksize * i
    stop = chunksize * (i + 1)
    return b[start:stop]