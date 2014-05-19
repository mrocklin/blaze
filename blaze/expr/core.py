from __future__ import absolute_import, division, print_function

__all__ = 'Expr',


class Expr(object):
    @property
    def args(self):
        return tuple(getattr(self, slot) for slot in self.__slots__)

    def isidentical(self, other):
        return type(self) == type(other) and self.args == other.args

    __eq__ = isidentical

    def __hash__(self):
        return hash((type(self), self.args))

    def __str__(self):
        return "%s(%s)" % (type(self).__name__, ', '.join(map(str, self.args)))

    def __repr__(self):
        return str(self)

    def traverse(self):
        """ Traverse over tree, yielding all subtrees and leaves """
        yield self
        traversals = (arg.traverse() if isinstance(arg, Expr) else [arg]
                        for arg in self.args)
        for trav in traversals:
            for item in trav:
                yield item

    def subs(self, d):
        """ Substitute terms in the tree

        >>> from blaze.expr.table import TableSymbol
        >>> t = TableSymbol('{name: string, amount: int, id: int}')
        >>> expr = t['amount'] + 3
        >>> expr.subs({3: 4, 'amount': 'id'})
        TableSymbol('{ name : string, amount : int32, id : int32 }')['id'] + 4
        """
        return subs(self, d)


def subs(o, d):
    if o in d:
        d = d.copy()
        other = d.pop(o)
        return subs(other, d)
    if isinstance(o, (tuple, list)):
        return type(o)([subs(arg, d) for arg in o])
    if hasattr(o, 'args'):
        newargs = [subs(arg, d) for arg in o.args]
        return type(o)(*newargs)

    return o
