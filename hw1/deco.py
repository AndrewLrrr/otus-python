#!/usr/bin/env python
# -*- coding: utf-8 -*-

from functools import update_wrapper


def disable(func):
    '''
    Disable a decorator by re-assigning the decorator's name
    to this function. For example, to turn off memoization:

    >>> memo = disable

    '''
    return func


def decorator(deco):
    '''
    Decorate a decorator so that it inherits the docstrings
    and stuff from the function it's decorating.
    '''
    def wrapper(func):
        return update_wrapper(deco(func), func)
    return update_wrapper(wrapper, deco)


@decorator
def countcalls(func):
    '''Decorator that counts calls made to the function decorated.'''
    def wrapper(*args, **kwargs):
        wrapper.calls += 1
        return func(*args, **kwargs)
    wrapper.calls = 0
    return wrapper


@decorator
def memo(func):
    '''
    Memoize a function so that it caches all return values for
    faster future lookups.
    '''
    def wrapper(*args, **kwargs):
        update_wrapper(wrapper, func)
        key = args, frozenset(kwargs.items()) if kwargs else args
        if key in wrapper.cache:
            return wrapper.cache[key]
        res = wrapper.cache[key] = func(*args, **kwargs)
        return res
    wrapper.cache = {}
    return wrapper


@decorator
def n_ary(func):
    '''
    Given binary function f(x, y), return an n_ary function such
    that f(x, y, z) = f(x, f(y,z)), etc. Also allow f(x) = x.
    '''
    def wrapper(x, *args):
        return x if not args else func(x, wrapper(*args))
    return wrapper


def trace(line):
    '''Trace calls made to function decorated.
    @trace("____")
    def fib(n):
        ....
    >>> fib(3)
     --> fib(3)
    ____ --> fib(2)
    ________ --> fib(1)
    ________ <-- fib(1) == 1
    ________ --> fib(0)
    ________ <-- fib(0) == 1
    ____ <-- fib(2) == 2
    ____ --> fib(1)
    ____ <-- fib(1) == 1
     <-- fib(3) == 3
    '''
    @decorator
    def tracer(func):
        def wrapper(*args, **kwargs):
            arguments = tuple(list(args) + ['%s=%s' % (k, v) for k, v in kwargs.items()]) if kwargs else args
            print '%s --> %s%s' % (line * wrapper.depth, func.__name__, arguments)
            wrapper.depth += 1
            res = func(*args, **kwargs)
            wrapper.depth -= 1
            print '%s <-- %s%s == %s' % (line * wrapper.depth, func.__name__, arguments, res)
            return res
        wrapper.depth = 0
        return wrapper
    return tracer

# tests disable
# memo = disable


@memo
@countcalls
@n_ary
def foo(a, b):
    return a + b


@countcalls
@memo
@n_ary
def bar(a, b):
    return a * b


@countcalls
@trace("####")
@memo
def fib(n):
    """Computes and prints the first n Fibonacci numbers."""
    return 1 if n <= 1 else fib(n-1) + fib(n-2)


@trace("----")
@memo
def my_fn(a, c=1, b=2):
    return c * (a + b)


def main():
    print foo(4, 3)
    print foo(4, 3, 2)
    print foo(4, 3)
    print "foo was called", foo.calls, "times"

    print bar(4, 3)
    print bar(4, 3, 2)
    print bar(4, 3, 2, 1)
    print "bar was called", bar.calls, "times"

    print fib.__doc__
    fib(3)
    print fib.calls, 'calls made'

    # my tests
    # print 'bar(5, 4, 3, 2, 1)', bar(5, 4, 3, 2, 1)
    # print my_fn(4, 5)
    # print my_fn(4, b=5)
    # print my_fn(4, b=5)
    # print my_fn(3, c=10)
    # print my_fn(3, b=5, c=10)
    # print my_fn(3, c=10, b=5)
    # print my_fn(4, 5)


if __name__ == '__main__':
    main()
