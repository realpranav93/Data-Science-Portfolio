import pandas as pd
import numpy as np


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