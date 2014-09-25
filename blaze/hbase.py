import happybase
import happybase_mock
from toolz.curried import take, merge, map, pipe, second, concat

from .dispatch import dispatch
from datashape import var, Record

def remove_column_families(d):
    """

    >>> remove_column_families({'cf1:foo': 1, 'cf2:bar': 2})
    {'bar': 1, 'foo': 2}
    """
    return dict((key.split(':')[-1], value) for key, value in d.items())


@dispatch((happybase.Table, happybase_mock.Table))
def discover(t, nrows=50):
    data = list(take(nrows, t.scan(batch_size=nrows)))

    names = pipe(data, map(second), concat, set, sorted)
    clean_names = ['row'] + [name.split(':')[-1] for name in names]

    data = [[row] + [d.get(name, None) for name in names]
            for row, d in data]

    tuple_type = discover(data).subshape[0][0]
    record_type = Record(list(zip(clean_names, tuple_type.dshapes)))

    return var * record_type
