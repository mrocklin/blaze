"""
Scalar
    Number
    Boolean

BinOp
UnaryOp
"""

from blaze.expr.core import Expr
import operator

class Scalar(Expr):
    pass


class Number(Scalar):
    def __eq__(self, other):
        return Eq(self, other)

    def __lt__(self, other):
        return LT(self, other)

    def __gt__(self, other):
        return GT(self, other)

    def __add__(self, other):
        return Add(self, other)

    def __radd__(self, other):
        return Add(other, self)

    def __mul__(self, other):
        return Mul(self, other)

    def __rmul__(self, other):
        return Mul(other, self)

    def __div__(self, other):
        return Div(self, other)

    def __rdiv__(self, other):
        return Div(other, self)

    def __sub_(self, other):
        return Sub(self, other)

    def __rsub__(self, other):
        return Sub(other, self)

    def __pow__(self, other):
        return Pow(self, other)

    def __rpow__(self, other):
        return Pow(other, self)

    def __mod__(self, other):
        return Mod(self, other)

    def __rmod__(self, other):
        return Mod(other, self)


class Boolean(Scalar):
    def __not__(self):
        return Not(self)

    def __and__(self, other):
        return And(self, other)

    def __or__(self, other):
        return Or(self, other)


class ScalarSymbol(Number):
    __slots__ = 'token',

    def __init__(self, token):
        self.token = token

    def eval(self, d):
        try:
            return d[self]
        except KeyError:
            raise ValueError("Symbol %s not found" % str(self))


class BinOp(Scalar):
    """ A column-wise Binary Operation

    >>> t = TableSymbol('{name: string, amount: int, id: int}')

    >>> data = [['Alice', 100, 1],
    ...         ['Bob', 200, 2],
    ...         ['Alice', 50, 3]]

    >>> from blaze.compute.python import compute
    >>> list(compute(t['amount'] * 10, data))
    [1000, 2000, 500]
    """
    __slots__ = 'lhs', 'rhs'

    def __init__(self, lhs, rhs):
        self.lhs = lhs
        self.rhs = rhs

    def __str__(self):
        return '%s %s %s' % (self.lhs, self.symbol, self.rhs)

    def eval(self, d):
        try:
            left = self.lhs.eval(d)
        except AttributeError:
            left = self.lhs
        try:
            right = self.rhs.eval(d)
        except AttributeError:
            right = self.rhs
        return self.op(left, right)


class Relational(BinOp, Boolean):
    @property
    def schema(self):
        return dshape('bool')


class Eq(Relational):
    symbol = '=='
    op = operator.eq


class GT(Relational):
    symbol = '>'
    op = operator.gt


class LT(Relational):
    symbol = '<'
    op = operator.lt


class And(Boolean):
    symbol = '&'
    op = operator.and_


class Or(Boolean):
    symbol = '|'
    op = operator.or_


class Arithmetic(BinOp, Number):
    @property
    def schema(self):
        # TODO: Infer schema based on input types
        return dshape('real')


class Add(Arithmetic):
    symbol = '+'
    op = operator.add


class Mul(Arithmetic):
    symbol = '*'
    op = operator.mul


class Sub(Arithmetic):
    symbol = '-'
    op = operator.sub


class Div(Arithmetic):
    symbol = '/'
    op = operator.truediv


class Pow(Arithmetic):
    symbol = '**'
    op = operator.pow


class Mod(Arithmetic):
    symbol = '%'
    op = operator.mod


class UnaryOp(Scalar):
    """ A column-wise Unary Operation

    >>> t = TableSymbol('{name: string, amount: int, id: int}')

    >>> data = [['Alice', 100, 1],
    ...         ['Bob', 200, 2],
    ...         ['Alice', 50, 3]]

    >>> from blaze.compute.python import compute
    >>> list(compute(log(t['amount']), data))  # doctest: +SKIP
    [4.605170185988092, 5.298317366548036, 3.912023005428146]
    """
    __slots__ = 'parent',

    def __init__(self, table):
        self.parent = table

    def __str__(self):
        return '%s(%s)' % (self.symbol, self.parent)

    @property
    def symbol(self):
        return type(self).__name__


class UnaryMath(Number, UnaryOp):
    def eval(self, d):
        try:
            parent = self.parent.eval(d)
        except AttributeError:
            parent = self.parent
        return _eval(self, parent)


import math
from numbers import Number
from multipledispatch import dispatch
@dispatch(UnaryMath, Number)
def _eval(op, x):
    op = getattr(math, op.symbol)
    return op(x)


class sin(UnaryMath): pass
class cos(UnaryMath): pass
class tan(UnaryMath): pass
class exp(UnaryMath): pass
class log(UnaryMath): pass
