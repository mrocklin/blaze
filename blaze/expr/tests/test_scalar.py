from blaze.expr.scalar import *
import math

def test_basic():
    x = ScalarSymbol('x')
    y = ScalarSymbol('y')

    expr = (x + y) * 3

    assert expr == Mul(Add(ScalarSymbol('x'), ScalarSymbol('y')), 3)


def test_eval():
    x = ScalarSymbol('x')
    y = ScalarSymbol('y')

    expr = (x + y) * 3

    assert expr.eval({x: 1, y: 2}) == (1 + 2) * 3

    assert sin(x).eval({x: 1}) == math.sin(1)
