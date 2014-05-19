from __future__ import absolute_import, division, print_function

from blaze.compute.spark import *
from blaze.expr.table import *

from pyspark import SparkContext

t = TableSymbol('{name: string, amount: int, id: int}')

data = [['Alice', 100, 1],
        ['Bob', 200, 2],
        ['Alice', 50, 3]]

t2 = TableSymbol('{name: string, city: string}')

data2 = [['Alice', 'Austin'],
         ['Bob', 'Boston']]

sc = SparkContext("local", "Simple App")

rdd = sc.parallelize(data)
rdd2 = sc.parallelize(data2)

#Web Commons Graph Example data
data_idx = [['A', 1],
            ['B', 2],
            ['C', 3]]

data_arc = [[1, 3],
            [2, 3],
            [3, 1]]

t_idx = TableSymbol('{name: string, node_id: int32}')

t_arc = TableSymbol('{node_out: int32, node_id: int32}')


def test_table():
    assert compute(t, rdd) == rdd


def test_projection():
    assert compute(t['name'], rdd).collect() == rdd.map(lambda x:
                                                        x[0]).collect()


def test_multicols_projection():
    assert compute(t[['amount', 'name']], rdd).collect() == [[100, 'Alice'],
                                                             [200, 'Bob'],
                                                             [50, 'Alice']]


def test_selection():
    assert compute(t[t['name'] == 'Alice'], rdd).collect() ==\
        rdd.filter(lambda x: x[0] == 'Alice').collect()


def test_join():

    joined = Join(t, t2, 'name')
    expected = [['Alice', 100, 1, 'Austin'],
                ['Bob', 200, 2, 'Boston'],
                ['Alice', 50, 3, 'Austin']]
    result = compute(joined, rdd, rdd2)
    assert all([i in expected for i in result.collect()])

def test_groupby():

    rddidx = sc.parallelize(data_idx)
    rddarc = sc.parallelize(data_arc)

    joined = Join(t_arc, t_idx, "node_id")

    result_blaze = compute(joined, {t_arc: rddarc, t_idx:rddidx})
    t = By(joined, joined['name'], joined['node_id'].count())
    a = compute(t, {t_arc: rddarc, t_idx:rddidx})
    in_degree = dict(a.collect())
    assert in_degree['C'] == 2
    assert in_degree['A'] == 1
