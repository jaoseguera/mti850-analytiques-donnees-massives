# -*- coding: utf-8 -*-
import matplotlib.pyplot as plt
import matplotlib.cm as cm
import numpy as np

def prepareSubplot(xticks, yticks, figsize=(10.5, 6), hideLabels=False, gridColor='#999999',
                gridWidth=1.0, subplots=(1, 1)):
    """Template for generating the plot layout."""
    plt.close()
    fig, axList = plt.subplots(subplots[0], subplots[1], figsize=figsize, facecolor='white',
                               edgecolor='white')
    if not isinstance(axList, np.ndarray):
        axList = np.array([axList])
        
    for ax in axList.flatten():
        ax.axes.tick_params(labelcolor='#999999', labelsize='10')
        for axis, ticks in [(ax.get_xaxis(), xticks), (ax.get_yaxis(), yticks)]:
            axis.set_ticks_position('none')
            axis.set_ticks(ticks)
            axis.label.set_color('#999999')
            if hideLabels: axis.set_ticklabels([])
        ax.grid(color=gridColor, linewidth=gridWidth, linestyle='-')
        map(lambda position: ax.spines[position].set_visible(False), ['bottom', 'top', 'left', 'right'])
        
    if axList.size == 1:
        axList = axList[0]  # Just return a single axes object for a regular plot
    return fig, axList
    
import pyspark
from pyspark.sql import DataFrame
import inspect
def printDataFrames(verbose=False):
    frames = inspect.getouterframes(inspect.currentframe())
    notebookGlobals = frames[1][0].f_globals
    for k,v in notebookGlobals.items():
        if isinstance(v, DataFrame) and '_' not in k:
            print ("{0}: {1}".format(k, v.columns) if verbose else "{0}".format(k) )

if __name__ == '__main__':
    printDataFrames()


def printLocalFunctions(verbose=False):
    frames = inspect.getouterframes(inspect.currentframe())
    notebookGlobals = frames[1][0].f_globals
    import types
    ourFunctions = [(k, v.__doc__) for k,v in notebookGlobals.items() if isinstance(v, types.FunctionType) and v.__module__ == '__main__']
    
    for k,v in ourFunctions:
        print ("** {0} **".format(k))
        if verbose:
            print (v)
        
if __name__ == '__main__':
    printLocalFunctions()
        
        
from collections.abc import Iterable
asSelf = lambda v: map(lambda r: r[0] if isinstance(r, Iterable) and len(r) == 1 else r, v)
