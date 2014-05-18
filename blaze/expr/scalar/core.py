
from blaze.expr.core import Expr

class Scalar(Expr):
    pass


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


class ScalarSymbol(Scalar):
    __slots__ = 'token',

    def __init__(self, token):
        self.token = token

    def eval(self, d):
        try:
            return d[self]
        except KeyError:
            raise ValueError("Symbol %s not found" % str(self))
