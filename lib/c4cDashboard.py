from pandas import DataFrame
from pandas import concat as concatDf
from pandas import read_json as jsonToDf
from functools import wraps
import matplotlib.pyplot as plt
from functools import lru_cache
import numpy as np

import io
import json
import base64

from .accessDB import getVarDf
from .accessDB import fixDataResults 
from .accessDB import queryData 

def commonAxisLabels(func):
    @wraps(func)
    def function_wrapper(*args, **kwargs):
        ax = func(*args, **kwargs)
        ax.set_xlabel('day of year', fontsize='large')
        ax.set_ylabel('CO2 footprint [kg]', fontsize='large')
        return ax
    return function_wrapper

# line chart, sum, tiems (ax[0,0])
@commonAxisLabels
def stackedBarChart(sumDf, ax):
    colOld = None
    X = sumDf.index
    for col in sumDf.columns:
        Y = sumDf.loc[:,col].values
        if (None == colOld):
            ax.bar(X, Y, label=col)
            colOld = col
        else:
            ax.bar(X, Y, label=col, bottom=sumDf.loc[:,colOld].values)
    ax.legend(loc='upper left', shadow=False, fontsize='large')
    return ax

# line chart (ax[0,1])
@commonAxisLabels
def lineChart(sumDf, ax):
    cumSumDf = sumDf.cumsum()
    X = cumSumDf.index
    scale0 = cumSumDf.loc[:,'fix'].values[-1] / cumSumDf.loc[:,'var'].values[-1]
    scale = round (scale0, 0)
    if (0 == scale):
        scale = round (scale0, 1)
        if (0 == scale):
            scale = round (scale0, 2)
            if (0 == scale):
                scale = 1
    for col in cumSumDf.columns:
        Y = cumSumDf.loc[:,col].values
        label = col
        if ('var' == label):
            label = label + " x " + str(scale)
            Y = Y * scale
        ax.plot(X, Y, label=label)
    ax.legend(loc='upper left', shadow=False, fontsize='large')
    return ax


# Pie chart per item(ax[1,0])
def pieChart(dataByTs, varPlotDict, fixDataDictSums, ax):
    varLabels = [label + "(" + str(dataByTs.sum()[label]) + " km)" for label in varPlotDict.sum().index]
    pie_labels = np.concatenate((varLabels, fixDataDictSums.columns))
    pie_values = np.concatenate((varPlotDict.sum().values, fixDataDictSums.values[0]))
    ax.pie(pie_values, labels=None, autopct='%1.1f%%', shadow=False, startangle=90)
    ax.axis('equal')
    ax.legend(loc='upper left', shadow=False, fontsize='large', labels = pie_labels)
    ax.set_xlabel('relative share of each item', fontsize='large')
    ax.set_ylabel('CO2 footprint [kg]', fontsize='large')
    return ax

# cumulative line chart for sum over all items (ax[1,1])
@commonAxisLabels
def lineChartSumAll(sumDf, ax):
    cumSumDf = sumDf.cumsum()
    X = cumSumDf.index
    ax.plot(X, cumSumDf.sum(axis=1).values, label='Total')
    ax.plot(X, [4800 for x in X], label='world average')
    ax.plot(X, [2700 for x in X], label='target')
    ax.plot(X, [10000 for x in X], label='german average')
    ax.legend(loc='upper left', shadow=False, fontsize='large')
    return ax


def figToBase64(fig):
    img = io.BytesIO()
    fig.savefig(img, format='png', bbox_inches='tight')
    img.seek(0)
    return base64.b64encode(img.getvalue())


# private <<<
# >>> public


@lru_cache(maxsize=32)
def getFigure(name, lastUpdate, beginTs=0, endTs=365):

    varPlotDict, dataByTs = getVarDf(name, beginTs, endTs)
    fixDataDictSums, fixDataDf = fixDataResults(name, beginTs, endTs)
    fixDf = DataFrame(fixDataDf.sum(axis=1), columns =['fix'])
    varDf = DataFrame(varPlotDict.sum(axis=1), columns =['var'])

    sumDf = fixDf.join(varDf).fillna(0)

    fig, ax = plt.subplots(2,2, constrained_layout=True)
    fig.suptitle('CO2 footprint report for: ' + name, fontsize=16, verticalalignment='top')
    fig.set_size_inches(15, 10)
    #fig.tight_layout(rect=[0, 0.03, 1, 0.95])

    # ax[0,0] = stacked bar chart of sum(var), sum(fix) per day
    ax[0,0] = stackedBarChart(sumDf, ax=ax[0,0])

    # ay[0,1] = line chart for cumulated sum for var and fix
    ax[0,1] = lineChart(sumDf, ax=ax[0,1])

    # a[1,0] pie chart percent of cumulated data of each item
    ax[1,0] = pieChart(dataByTs, varPlotDict, fixDataDictSums, ax=ax[1,0])
    # a[1,1] line chart of sum over each item cumulated per day
    ax[1,1] = lineChartSumAll(sumDf, ax=ax[1,1])

    # << axis lables
    return figAsHtml(fig)


@lru_cache(maxsize=32)
def getCompare(name, queryJson = None, beginTs=0, endTs=365):
    fixDocDf, dataDocByTs = queryData(name, beginTs, endTs)


    varPlotDict, dataByTs = getVarDf(name, beginTs, endTs)
    fixDataDictSums, fixDataDf = fixDataResults(name, beginTs, endTs)
    fixDf = DataFrame(fixDataDf.sum(axis=1), columns =['fix'])
    varDf = DataFrame(varPlotDict.sum(axis=1), columns =['var'])

    sumDf = fixDf.join(varDf).fillna(0)

    replaceDict = dict()
    replaceTxt = "No change specified."
    if (None != queryJson):
        queryDict = json.loads(queryJson)
        replaceDict = {k:[float(fixDocDf.loc[0,k]), float(queryDict[k])] for k in queryDict if k in [*fixDocDf.columns]}
        # >>> details about what was changed
        replaceTxt = "CO2 footptint change:," + "".join(
            [str(k) + ": " + str(replaceDict[k][0])+"->"+str(replaceDict[k][1]) + 
                      " (" + str(round((replaceDict[k][1]/replaceDict[k][0] - 1)* 100, 0)) + "%),"
                for k in replaceDict]
            )
        # details about what was changed <<<<

    fig, ax = plt.subplots(2,2, constrained_layout=True)
    fig.suptitle('CO2 footprint comparision, change for: ' + name, fontsize=16, verticalalignment='top')
    fig.set_size_inches(15, 10)

    ax[0,0] = pieChart(dataByTs, varPlotDict, fixDataDictSums, ax=ax[0,0])
    ax[1,1] = lineChartSumAll(sumDf, ax=ax[1,1])

    oldValues = np.concatenate((varPlotDict.sum().values, fixDataDictSums.values[0]))
    varLabels = [label + "(" + str(dataByTs.sum()[label]) + " km)" for label in varPlotDict.sum().index]
    compareColumns = np.concatenate((varLabels, fixDataDictSums.columns))
    # pie_values = np.concatenate((varPlotDict.sum().values, fixDataDictSums.values[0]))
    for field in varPlotDict.columns:
        if (field in replaceDict):
            c = replaceDict[field][1] / fixDocDf.loc[0,field]
            varPlotDict[field] = varPlotDict[field].multiply(c)
    for field in fixDataDictSums.columns:
        if (field in replaceDict):
            c = replaceDict[field][1] / fixDocDf.loc[0,field]
            fixDataDictSums[field] = fixDataDictSums[field].multiply(c)
    for field in fixDataDf.columns:
        if (field in replaceDict):
            c = replaceDict[field][1] / fixDocDf.loc[0,field]
            fixDataDf[field] = fixDataDf[field].multiply(c)

    ax[0,1] = pieChart(dataByTs, varPlotDict, fixDataDictSums, ax=ax[0,1])

    newValues = np.concatenate((varPlotDict.sum().values, fixDataDictSums.values[0]))
    X = np.arange(0, len(compareColumns))
    width = 0.3
    ax[1,0].bar(X - width/2, oldValues, label='original', width = width)
    ax[1,0].bar(X + width/2, newValues, label='modified', width = width) #, bottom=oldValues)
    compareColumns = np.concatenate((['a'], compareColumns))
    ax[1,0].set_xticklabels(compareColumns)
    ax[1,0].legend(loc='upper left', shadow=False, fontsize='large', labels = ['original', 'predicted'])

    fixDf = DataFrame(fixDataDf.sum(axis=1), columns =['fix'])
    varDf = DataFrame(varPlotDict.sum(axis=1), columns =['var'])
    sumDf = fixDf.join(varDf).fillna(0)
    cumSumDf = sumDf.cumsum()
    X = cumSumDf.index
    ax[1,1].plot(X, cumSumDf.sum(axis=1).values, label='predicted')
    ax[1,1].legend(loc='upper left', shadow=False, fontsize='large')

    ax[0,0].set_title('Original share of CO2 footprint by source.')
    ax[0,1].set_title('Predicted share of CO2 footprint by source.')
    ax[1,0].set_title('Original vers predicted CO2 footprint by source.')
    ax[1,1].set_:title('Original vers predicted CO2 footprint in total.')
    ax[1,0].set_ylabel('CO2 footprint [kg]', fontsize='large')

    return replaceTxt, figAsHtml(fig)

def figAsHtml(fig):
    encoded = figToBase64(fig)
    return '<img src="data:image/png;base64, {}">'.format(encoded.decode('utf-8'))


