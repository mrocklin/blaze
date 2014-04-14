from .data_descriptor import DDesc
from .dynd_data_descriptor import DyND_DDesc
from ..utils import partition_all
import h5py
import datashape
from dynd import nd
import numpy as np
from itertools import chain

h5py_attributes = ['chunks', 'compression', 'compression_opts', 'dtype',
                   'fillvalue', 'fletcher32', 'maxshape', 'shape']

def log(s):
    with open('test.txt', 'a') as f:
        f.write(str(s))
        f.write('\n')

def dshape_from_h5py(dset):
    if not dset:
        raise ValueError('Dataset does not yet exist')
    dtype = dset.dtype
    shape = dset.shape
    return datashape.from_numpy(dset.shape, dset.dtype)


class H5PY_DDesc(DDesc):
    """
    A Blaze data descriptor which exposes an HDF5 file.

    Parameters
    ----------
    path: string
        Location of hdf5 file on disk
    datapath: string
        Location of array dataset in hdf5
    mode : string
        r, w, rw+
    schema: string or Datashape
        a datashape describing one row of the data
    dshape: string or Datashape
        a datashape describing the data
    **kwargs:
        Options to send to h5py - see h5py.File.create_dataset for options
    """

    def __init__(self, path, datapath, mode='r', schema=None, dshape=None, **kwargs):
        self.path = path
        self.datapath = datapath
        self.mode = mode

        import os
        if not os.path.exists(path) and ('w' in mode or 'a' in mode):
            f = h5py.File(path, 'w')
            f.close()
            assert os.path.exists(path)

        if schema and not dshape:
            dshape = 'var * ' + str(schema)

        # TODO: provide sane defaults for kwargs
        # Notably chunks and maxshape
        log('dshape logic')
        if dshape:
            dshape = datashape.dshape(dshape)
            shape = dshape.shape
            dtype = datashape.to_numpy_dtype(dshape[-1])
            if shape[0] == datashape.Var():
                kwargs['chunks'] = True
                kwargs['maxshape'] = kwargs.get('maxshape', (None,) + shape[1:])
                shape = (0,) + tuple(map(int, shape[1:]))
        else:
            with h5py.File(path, 'r') as f:
                dset = f.get(datapath)
                if not dset:
                    raise ValueError('No dataset or dshape provided')

                dtype = dset.dtype
                shape = dset.shape
                dshape = datashape.from_numpy(dset.shape, dset.dtype)

        log(self.mode)
        log(path)
        import os
        log(os.path.exists(path))
        with h5py.File(path, self.mode) as f:
            log(f)
            log(f.filename)
            dset = f.require_dataset(datapath, shape, dtype, **kwargs)
            log(dset)

            # TODO: Verify that DDesc and HDF5 dshapes are consistent
            found_dshape = dshape_from_h5py(dset)
            if (str(dshape).replace('var', '0') !=
                    str(found_dshape).replace('var', '0')):
                raise ValueError('Inconsistent datashapes.'
                        'Given: %s\nFound: %s' % (dshape,
                                                  dshape_from_h5py(dset)))

        log('attributes')
        attributes = self.attributes()
        if attributes['chunks']:
            # is there a better way to do this?
            words = str(dshape).split(' * ')
            dshape = 'var * ' + ' * '.join(words[1:])
            dshape = datashape.dshape(dshape)

        self._dshape = dshape

        # Runtime checking
        import os
        log('runtime checks')
        assert os.path.exists(path)

        with h5py.File(self.path, 'r') as f:
            log(f.get(self.datapath))
            assert f.get(self.datapath)

    def _extend(self, seq):
        self.extend_chunks(partition_all(100, seq))

    @property
    def schema(self):
        return ' * '.join(str(self.dshape).split(' * ')[1:])

    def attributes(self):
        with h5py.File(self.path, 'r') as f:
            arr = f[self.datapath]
            result = dict((attr, getattr(arr, attr))
                            for attr in h5py_attributes)
        return result

    def __getitem__(self, key):
        with h5py.File(self.path, mode='r') as f:
            arr = f[self.datapath]
            result = np.asarray(arr[key])
        return nd.asarray(result, access='readonly')

    def iterchunks(self, blen=100):
        with h5py.File(self.path, mode='r') as f:
            arr = f[self.datapath]
            for i in range(0, arr.shape[0], blen):
                yield nd.asarray(np.array(arr[i:i+blen]), access='readonly')

    def __iter__(self):
        return chain.from_iterable(self.iterchunks())

    @property
    def dshape(self):
        return self._dshape

    @property
    def capabilities(self):
        return {'immutable': False,
                'deferred': False,
                'persistent': True,
                'appendable': True,
                'remote': False}

    def dynd_arr(self):
        return self[:]

    def _extend_chunks(self, chunks):
        if 'w' not in self.mode and 'a' not in self.mode:
            raise ValueError('Read only')

        with h5py.File(self.path, mode=self.mode) as f:
            dset = f.get(self.datapath)
            if not dset:
                raise ValueError('Dataset not previously created')
            dtype = dset.dtype
            shape = dset.shape
            for chunk in chunks:
                arr = np.array(chunk, dtype=dtype)
                shape = list(dset.shape)
                shape[0] += len(arr)
                dset.resize(shape)
                dset[-len(arr):] = arr
