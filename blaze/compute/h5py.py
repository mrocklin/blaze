from __future__ import absolute_import, division, print_function

import h5py
from blaze.expr.table import *
from datashape import Record
from ..dispatch import dispatch
from .chunks import Chunks


@dispatch(Head, h5py.Dataset)
def compute_one(expr, data, **kwargs):
    return data[:expr.n]


@dispatch((RowWise, Distinct, Reduction, By, count, Label, ReLabel, nunique),
          h5py.Dataset)
def compute_one(c, t, **kwargs):
    return compute_one(c, Chunks(t), **kwargs)
