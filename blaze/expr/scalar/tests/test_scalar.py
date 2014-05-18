from blaze.expr.scalar import *
import math

x = NumberSymbol('x')
y = NumberSymbol('y')

def test_basic():
    expr = (x + y) * 3

    assert expr == Mul(Add(NumberSymbol('x'), NumberSymbol('y')), 3)


def test_eval():
    expr = (x + y) * 3

    assert expr.eval({x: 1, y: 2}) == (1 + 2) * 3

    assert sin(x).eval({x: 1}) == math.sin(1)

def test_compile():
    expr = (x + y) * 3

    assert '(x + y) * 3' in expr.compile_str()
