#!/usr/bin/env python
# coding: utf-8


def ex_sorted(iterable, cmp = None, key = None, reverse = False, none = True):
    """
    Args:
        obj (iterable): a list to be sorted
        key (function): key function to compare the component in iterable.
                        unlike sorted, key can return None in this function.
                        key(e) = None will be placed in the latter part
                        if none is True
        reverse (bool)
        none_large (bool): 

    Returns:
        list  # not iterable now...
    """
    l_sorted = []
    l_none = []

    for e in iterable:
        if key(e) is None:
            l_none.append(e)
        else:
            l_sorted.append(e)

    l_sorted = sorted(l_sorted, cmp, key, reverse)
    if reverse == none:
        return l_none + l_sorted
    else:
        return l_sorted + l_none

