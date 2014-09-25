from blaze.hbase import *
import happybase_mock as happybase
from datashape import dshape

conn = happybase.Connection('localhost')

conn.create_table('t1', {'cf': dict()})
conn.create_table('t2', {'cf': dict()})

t = conn.table('t1')
t.put('0', {'cf:name': 'Alice', 'cf:amount': 100})
t.put('1', {'cf:name': 'Bob', 'cf:amount': 200})
t.put('3', {'cf:name': 'Alice', 'cf:amount': 300})

def test_discover():
    assert discover(t) == dshape('var * {row: int64, amount: int64, name: string}')

def test_into_list():
    assert into(set, t) == set([('0', 100, 'Alice'),
                                ('1', 200, 'Bob'),
                                ('3', 300, 'Alice')])
