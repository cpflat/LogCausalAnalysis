#!/usr/bin/env python
# coding: utf-8


ESC_LETTER = "*@" # including back slash

def split_igesc(string, spl):
    l_esc = ["\\" + w for w in "\\" + ESC_LETTER]
    temp_str = string
    spl_len = len(spl)
    temp_ww = []
    ret = []
    while len(temp_str) >= spl_len:
        if temp_str[0:2] in l_esc:
            temp_ww.append(temp_str[:2])
            temp_str = temp_str[2:]
        elif fmatch(temp_str, spl):
            ret.append("".join(temp_ww))
            temp_ww = []
            temp_str = temp_str[len(spl):]
        else:
            temp_ww.append(temp_str[0])
            temp_str = temp_str[1:]
    else:
        ret.append("".join(temp_ww) + temp_str)
    return ret


def add_esc(string):
    for w in "\\" + ESC_LETTER:
        string = string.replace(w, "\\" + w)
    return string


def restore_esc(string):
    for w in ESC_LETTER + "\\":
        string = string.replace("\\" + w, w)
    return string


def fmatch(string, match):
    """Test the forward part of string matches another string or not.
    """
    assert len(string) >= len(match)
    return string[:len(match)] == match


