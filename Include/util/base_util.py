# -*- coding: utf-8 -*-
def get_str_btw(s, f, b):
    """截取字符串"""
    par = s.partition(f)
    return (par[2].partition(b))[0][:]
