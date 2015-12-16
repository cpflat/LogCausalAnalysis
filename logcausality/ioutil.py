#!/usr/bin/env python
# coding: utf-8


def table(data, label = None, spl = " ", fill = " "):
    # data : [row = [str, str, ...], row1, ...]

    l_length = [max(column, key = lambda x: len(str(x)))
            for column in zip(data)]
    l_buf = []
    
    if label is not None:
        l_buf.append(spl.join([elem.ljust(length)
                for elem, length in zip(label, l_length)]))

    for row in data:
        temp_buf = []
        for elem, length in zip(row, l_length):
            temp_buf.append(elem.ljust(length))
        l_buf.append(spl.join(temp_buf))

    return "\n".join(l_buf)

