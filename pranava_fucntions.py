import pandas as pd
import numpy as np
import datetime


def get_unique_cnt(x):
    return x.unique().size
def get_decile(x):
    import numpy as np
    return np.percentile(x,np.arange(0,100,10))
def remove_nan_list(l):
    """
    i: list, o: list
    description : removes a nan from the list and returns the final list
    """
    return [a for a in l if a == a ]


def get_fiscal_quarter_ms(x, offset = 0):
    """
    x: date,
     
    offset  -- measure to offset the quarter to previous or future quarters (default=1)

    ouptut: Returns a microsoft fiscal quarter number from a given date in format fyqq e.g. 202101 for FY21 quarter1
    """ 
    from pandas.tseries.offsets import DateOffset,BQuarterEnd
    if isinstance(x, datetime.datetime):
        if offset > 0: 
            offset += 1

        f_x = x + BQuarterEnd(offset)
        
        fiscal_quarter = f_x.to_period('Q-Jul')

    else: 
        print("Input is not a date") 

    return fiscal_quarter


import re
import inspect
from functools import wraps

def str_to_int(fn):
    """finds all the integers in the string ouptut of a function and converts it into int. Is a wrapper for other functions"""
    @wraps(fn)
    def wrapper(*args, **kwargs):
        return int(''.join(re.findall("\d+", fn(*args, **kwargs))))
    return wrapper

   
@str_to_int
def intvalue_before_str(s, string):
    """
    s = input string 
    string = pattern which needs to be matched
    finds the pattern in string and extracts the test before it and with str_to_int decorator gets converted into int
    """
    if isinstance(s, str): 
        if re.search("([\d,.-_]+)%s" % (string), s).group(1): 
            return re.search("([\d,.-_]+)%s" % (string), s).group(1)
        else: 
            return '0'
    else: 
        return '0'

@str_to_int
def intvalue_after_str(s, string):
    """
    s = input string 
    string = pattern which needs to be matched
    finds the pattern in string and extracts the test after it and with str_to_int decorator gets converted into int
    """
    if isinstance(s, str): 
        if re.search("([\d,.-_]+)%s([\d,.-_]+)" % (string), s).group(2): 
            return re.search("([\d,.-_]+)%s([\d,.-_]+)" % (string), s).group(2)
        else: 
            return '0'
    else: 
        return '0'

@str_to_int
def value_between_str(s, start_str, end_str):
    if isinstance(s, str): 
        if re.search('%s(.*)%s' % (start_str,end_str), s): 
            return re.search('%s(.*)%s' % (start_str,end_str), s).group(1)
        else: 
            return ''
    else: 
        return ''

def get_diff_list(a, b):
    return list(set(a) - set(b))