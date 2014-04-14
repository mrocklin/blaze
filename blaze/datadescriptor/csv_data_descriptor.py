from __future__ import absolute_import, division, print_function

import csv
import itertools as it
import os

import datashape
from dynd import nd

from .. import py2help
from .data_descriptor import DDesc
from .dynd_data_descriptor import DyND_DDesc
from .as_py import ddesc_as_py
from .util import coerce, coerce_record_to_row
from ..utils import partition_all, nth
from .. import py2help


def open_file(path, mode, has_header):
    """Return a file handler positionated at the first valid line."""
    csvfile = open(path, mode=mode)
    if has_header:
        csvfile.readline()
    return csvfile


class CSV_DDesc(DDesc):
    """
    A Blaze data descriptor which exposes a CSV file.

    Parameters
    ----------
    path : string
        A path string for the CSV file.
    schema : string or datashape
        A datashape (or its string representation) of the schema
        in the CSV file.
    dialect : string or csv.Dialect instance
        The dialect as understood by the `csv` module in Python standard
        library.  If not specified, a value is guessed.
    has_header : boolean
        Whether the CSV file has a header or not.  If not specified a value
        is guessed.
    """

    def __init__(self, path, mode='r', schema=None, dialect=None,
            has_header=None, **kwargs):
        if 'r' in mode and os.path.isfile(path) is not True:
            raise ValueError('CSV file "%s" does not exist' % path)
        self.path = path
        self.mode = mode
        csvfile = open(path, mode=self.mode)

        # Handle Schema
        if isinstance(schema, py2help._strtypes):
            schema = datashape.dshape(schema)
        if not schema:
            # TODO: schema detection from first row
            raise ValueError('Need schema')
        self.schema = str(schema)

        # Guess Dialect if not an input
        if dialect is None and 'r' in mode:
            sniffer = csv.Sniffer()
            try:
                dialect = sniffer.sniff(csvfile.read(1024))
            except:
                pass
        if dialect is None:
            dialect = csv.get_dialect('excel')
        elif isinstance(dialect, py2help.basestring):
            dialect = csv.get_dialect(dialect)
        # Convert dialect to dictionary
        self.dialect = dict((key, getattr(dialect, key))
                            for key in dir(dialect) if not key.startswith('_'))

        # Update dialect with any keyword arguments passed in
        # E.g. allow user to override with delimiter=','
        for k, v in kwargs.items():
            if k in self.dialect:
                self.dialect[k] = v

        # Handle Header
        if has_header is None and mode != 'w':
            # Guess whether the file has a header or not
            sniffer = csv.Sniffer()
            csvfile.seek(0)
            sample = csvfile.read(1024)
            try:
                self.has_header = sniffer.has_header(sample)
            except:
                self.has_header = has_header

        else:
            self.has_header = has_header

        csvfile.close()

    @property
    def dshape(self):
        return datashape.DataShape(datashape.Var(), self.schema)

    @property
    def capabilities(self):
        """The capabilities for the csv data descriptor."""
        return {'immutable': False,
                'deferred': False,
                'persistent': True,
                'appendable': True,
                'remote': False}

    def dynd_arr(self):
        with open_file(self.path, self.mode, self.has_header) as csvfile:
            seq = csv.reader(csvfile, **self.dialect)
            return nd.array(seq, dtype=self.schema)

    def __array__(self):
        return nd.as_numpy(self.dynd_arr())

    def __getitem__(self, key):
        with open_file(self.path, self.mode, self.has_header) as csvfile:
            if isinstance(key, py2help._inttypes):
                line = nth(key, csvfile)
                result = next(csv.reader([line], **self.dialect))
            elif isinstance(key, slice):
                start, stop, step = key.start, key.stop, key.step
                result = list(csv.reader(it.islice(csvfile, start, stop, step),
                                         **self.dialect))
            else:
                raise IndexError("key '%r' is not valid" % key)
        return coerce(self.schema, result)

    def __iter__(self):
        with open(self.path, self.mode) as f:
            if self.has_header:
                next(f)  # burn header
            for row in csv.reader(f, **self.dialect):
                yield coerce(self.schema, row)

    def _extend(self, rows):
        rows = iter(rows)
        with open_file(self.path, self.mode, self.has_header) as f:
            row = next(rows)
            if isinstance(row, dict):
                schema = datashape.dshape(self.schema)
                row = coerce_record_to_row(schema, row)
                rows = (coerce_record_to_row(schema, row) for row in rows)

            # Write all rows to file
            f.seek(0, os.SEEK_END)  # go to the end of the file
            writer = csv.writer(f, **self.dialect)
            writer.writerow(row)
            writer.writerows(rows)

    def _iterchunks(self, blen=100, start=None, stop=None):
        """Return chunks of size `blen` (in leading dimension).

        Parameters
        ----------
        blen : int
            The length, in rows, of the buffers that are returned.
        start : int
            Where the iterator starts.  The default is to start at the
            beginning.
        stop : int
            Where the iterator stops. The default is to stop at the end.

        Returns
        -------
        out : iterable
            This iterable returns buffers as DyND arrays,

        """
        with open_file(self.path, self.mode, self.has_header) as f:
            f = it.islice(csv.reader(f, **self.dialect), start, stop)
            for rows in partition_all(blen, f):
                yield rows

    def remove(self):
        """Remove the persistent storage."""
        os.unlink(self.path)
