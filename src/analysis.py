import pandas as pd
import numpy as np

SECONDS_PER_LUMISECTION = 23.3

def homogenizeDataPerLumisection(data, startTimestamp, stopTimestamp):
    """
    Laurent Petre
    WORKS ON A SINGLE CHAMBER AT A TIME
    given the json formatted query response, 
    1. Build one DataFrame per metric, indexed by a datetime
    2. Merge them horizontally, all the metrics are now columns (i.e. vMon-g1top, v0Set-g1top, vMon-g1bot, ...)
    3. Add indices at the beginning of each lumisection
    4. Fill the voids with the previous valid values (pandas.FillN(method='ffill'))
    5. Calculate total voltage applied and the equivalent divider current
    """
    dfs = []
    for metric in data:
        variable = metric["target"].split(",")[0]
        electrode = metric["target"].split(":")[-1].lower()
        df = pd.DataFrame(metric["datapoints"], columns=[f"{variable}-{electrode}", "datetime"])
        df["datetime"] = pd.to_datetime(df["datetime"]*1000000, utc=True)
        df = df.set_index("datetime")
        dfs.append(df)

    df = pd.concat(dfs, axis=1)

    lumibounds = pd.date_range(startTimestamp*1000000000, stopTimestamp*1000000000, freq=f"{SECONDS_PER_LUMISECTION*1000000}us", tz="UTC") # FIXME approximative, use actual boundaries from OMS
    df = df.reindex(df.index.union(lumibounds)) # sort=True can be ensured in newer Pandas versions
    df.fillna(method="ffill", inplace=True)
        
    df["timestamp"] = df.index.astype(np.int64)/1e9
    df["lumisection"] = (df["timestamp"] - df["timestamp"][0])//SECONDS_PER_LUMISECTION # FIXME approximative, use actual boundaries from OMS
    df["vMon"] = df[list(set(list(df.filter(regex='vMon'))))].sum(axis=1)
    df["v0Set"] = df[list(set(list(df.filter(regex='v0Set'))))].sum(axis=1)

    df["iEqMon"] = df["vMon"]/4.7
    df["iEqSet"] = df["v0Set"]/4.7
    
    return df
