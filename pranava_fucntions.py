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