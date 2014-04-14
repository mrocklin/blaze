from blaze.datadescriptor import H5PY_DDesc, DyND_DDesc, DDesc
from blaze.datadescriptor.util import openfile
import unittest
import tempfile
import os
from dynd import nd
import h5py
import numpy as np
import random

from threading import Lock

lock = Lock()

def random_filename(ext):
    fn = 'test-1.%s' % ext
    while os.path.exists(fn):
        fn = 'rand_%d.%s' % (random.randint(2, 100000), ext)
    return fn


def log(s):
    with open('test.txt', 'a') as f:
        f.write(s)
        f.write('\n')


class SingleTestClass(unittest.TestCase):
    def setUp(self):
        self.filename = random_filename('h5')

    def tearDown(self):
        if os.path.exists(self.filename):
            os.remove(self.filename)

    """
    def test_creation(self):
        dd = H5PY_DDesc(self.filename, 'data', 'w', dshape='2 * 2 * int32')

        with h5py.File(self.filename, 'r') as f:
            d = f['data']
            self.assertEquals(d.dtype.name, 'int32')

        self.assertRaises(ValueError, lambda: H5PY_DDesc('bar.hdf5', 'foo'))
        """

    def test_existing_array(self):
        log('test_existing_array')

        with h5py.File(self.filename, 'w') as f:
            d = f.create_dataset('data', (3, 3), dtype='i4',
                                 chunks=True, maxshape=(None, 3))
            d[:] = 1

        dd = H5PY_DDesc(self.filename, '/data', mode='a')

        known = {'chunks': True,
                 'maxshape': (None, 3),
                 'compression': None}
        attrs = dd.attributes()
        assert attrs['chunks']
        self.assertEquals(attrs['maxshape'], (None, 3))
        assert not attrs['compression']

        self.assertEquals(str(dd.dshape), 'var * 3 * int32')

    def test_extend_chunks(self):
        log('test_extend_chunks')
        with h5py.File(self.filename, 'w') as f:
            d = f.create_dataset('data', (3, 3), dtype='i4',
                                 chunks=True, maxshape=(None, 3))
            d[:] = 1

        dd = H5PY_DDesc(self.filename, '/data', mode='a')

        chunks = [nd.array([[1, 2, 3]], dtype='1 * 3 * int32'),
                  nd.array([[4, 5, 6]], dtype='1 * 3 * int32')]

        dd.extend_chunks(chunks)

        result = dd.dynd_arr()[-2:, :]
        expected = nd.array([[1, 2, 3],
                             [4, 5, 6]], dtype='strided * strided * int32')

        self.assertEquals(nd.as_py(result), nd.as_py(expected))

    def test_iterchunks(self):
        log('test_iterchunks')
        with h5py.File(self.filename, 'w') as f:
            d = f.create_dataset('data', (3, 3), dtype='i8')
            d[:] = 1
        dd = H5PY_DDesc(self.filename, '/data')
        assert all(isinstance(chunk, nd.array) for chunk in dd.iterchunks())

def test_extend():
    log('test_extend')
    with openfile('h5') as filename:
        log(filename)
        log('written to file')
        dd = H5PY_DDesc(filename, '/data', 'a', schema='2 * int32')
        log('test_extend')
        dd.extend([(1, 1), (2, 2)])
        log('test_extend')

        results = list(dd)

        assert nd.as_py(results[0]) == [1, 1]
        assert nd.as_py(results[1]) == [2, 2]


def test_schema():
    with openfile('h5') as filename:
        dd = H5PY_DDesc(filename, '/data', 'a', schema='2 * int32')

        assert str(dd.schema) == '2 * int32'
        assert str(dd.dshape) == 'var * 2 * int32'


def test_dshape():
    with openfile('h5') as filename:
        log('test_dshape')
        log(filename)

        dd = H5PY_DDesc(filename, '/data2', 'w', dshape='var * 2 * int32')

        log('after h5py')

        assert str(dd.schema) == '2 * int32'
        assert str(dd.dshape) == 'var * 2 * int32'

