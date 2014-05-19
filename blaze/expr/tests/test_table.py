from __future__ import absolute_import, division, print_function

from blaze.expr.table import *
from datashape import dshape


def test_dshape():
    t = TableSymbol('{name: string, amount: int}')
    assert t.dshape == dshape('var * {name: string, amount: int}')


def test_eq():
    assert TableSymbol('{a: string, b: int}') == \
            TableSymbol('{a: string, b: int}')
    assert TableSymbol('{b: string, a: int}') != \
            TableSymbol('{a: string, b: int}')


def test_column():
    t = TableSymbol('{name: string, amount: int}')
    assert t.columns == ['name', 'amount']


def test_Projection():
    t = TableSymbol('{name: string, amount: int, id: int32}')
    p = Projection(t, ['amount', 'name'])
    assert p.schema == dshape('{amount: int32, name: string}')
    print(t['amount'].dshape)
    print(dshape('var * int32'))
    assert t['amount'].dshape == dshape('var * {amount: int32}')


def test_indexing():
    t = TableSymbol('{name: string, amount: int, id: int}')
    assert t[['amount', 'id']] == Projection(t, ['amount', 'id'])
    assert t['amount'].isidentical(Column(t, 'amount'))


def test_relational():
    t = TableSymbol('{name: string, amount: int, id: int}')

    r = t['name'] == 'Alice'

    assert r.dshape == dshape('var * bool')


def test_selection():
    t = TableSymbol('{name: string, amount: int, id: int}')

    s = Selection(t, Eq(t['name'], 'Alice'))

    assert s.dshape == t.dshape


def test_selection_by_indexing():
    t = TableSymbol('{name: string, amount: int, id: int}')

    result = t[t['name'] == 'Alice']
    expected = Selection(t, Eq(Column(t, 'name'), 'Alice'))
    assert str(result) == str(expected)


def test_columnwise():
    t = TableSymbol('{x: real, y: real, z: real}')
    x, y, z = t['x'], t['y'], t['z']
    expr = z % x * y + z ** 2
    assert isinstance(expr, ColumnWise)


def test_str():
    import re
    t = TableSymbol('{name: string, amount: int, id: int}')
    expr = t[t['amount'] < 0]['name'] * 2
    print(str(expr))
    assert '<class' not in str(expr)
    assert not re.search('0x[0-9a-f]+', str(expr))

    assert eval(str(expr)) == expr

    assert '*' in repr(expr)


def test_join():
    t = TableSymbol('{name: string, amount: int}')
    s = TableSymbol('{name: string, id: int}')
    j = Join(t, s, 'name', 'name')

    assert j.schema == dshape('{name: string, amount: int, id: int}')

    assert Join(t, s, 'name') == Join(t, s, 'name')


def test_traverse():
    t = TableSymbol('{name: string, amount: int}')
    assert t in list(t.traverse())

    expr = t[t['amount'] < 0]['name']
    trav = list(expr.traverse())
    assert t['amount'] in trav
    assert (t['amount'] < 0) in trav


def test_unary_ops():
    t = TableSymbol('{name: string, amount: int}')
    expr = cos(exp(t['amount']))
    assert 'cos' in str(expr)


def test_reduction():
    t = TableSymbol('{name: string, amount: int32}')
    r = sum(t['amount'])
    print(type(r.dshape))
    print(type(dshape('int32')))
    assert r.dshape == dshape('int32')


def test_reduce_by():
    t = TableSymbol('{name: string, amount: int32, id: int32}')
    r = By(t, t['name'], sum(t['amount']))


def test_columnwise():
    t = TableSymbol('{name: string, amount: int32, id: int32}')
    result = t['amount'] + 1
    expected = ColumnWise(Add(_0), 1),
                          (t['amount'],))

    result = (t['amount'] + 1) * (t['id'] ** t['amount'])
    expected = ColumnWise(Mul(Add(_0, 1),
                              Pow(_1, _0)),
                          (t['amount'], t['id']))
