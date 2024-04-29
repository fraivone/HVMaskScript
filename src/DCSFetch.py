import os
import pandas as pd
import numpy as np
import time
from urllib import request
import json


class DCSchamberHVInfo:
    """
    Class to ease the querying of the DCS bridge for HV data
    Timestamps always in UTC format.
    Querying the DB is usually done in batches.
    Each batch contains <UNITS_PER_BATCH> at most, 
    so that the server timeouts are axvoided.
    Timestamps are always expressed in milliseconds when returned by the DCS
    Timestamps in the DCS DB query have to be in seconds
    
    debug example from command prompt:
    
    wget --output-document=output.txt --post-data='{"from": 1713414214, "to": 1713429712, "targets": [{"metric": "vMon", "tag": "GE11-M-01L1:HV:G3TOP"}]}'   --header='Content-Type:application/json'   $DCS_BRIDGE
    
    server response (stored in output.txt)
    [ 
    {'target':'vMon,GE11-P-16L1:HV:DRIFT', 'datapoints':[[VALUE,TIMESTAMP],...,[VALUE,TIMESTAMP]]},
    {},
    ...,
    ]
    """
    
    def __init__(self, allChambers:list, startTimestamp:int, stopTimestamp:int):
        self.DCS_URL = os.environ['DCS_BRIDGE']
        self.UNITS_PER_BATCH = 40
        self.SECONDS_PER_LUMISECTION = 23.3
        self.ELECTRODE_NAMES = {"DRIFT","G1TOP","G2TOP","G3TOP","G1BOT","G2BOT","G3BOT"}

        self.startTimestamp = startTimestamp
        self.stopTimestamp = stopTimestamp #s
        self.allChambers = allChambers #s
        self.HVDataFrame : pd.DataFrame
        self.batches = self.generate_batches(self.allChambers, self.UNITS_PER_BATCH)

    def generate_batches(self,lst, n):
        return [lst[i:i+n] for i in range(0, len(lst), n)]
    
    def fetchData(self):
        DataFrames = []
        sec_per_ls = self.SECONDS_PER_LUMISECTION
        for idx,ch_batch in enumerate(self.batches):
            print(f"Fetching HV data for batch {idx+1}/{len(self.batches)}", end="... ")
            t0 = time.time()
            query = self._generateQuery(ch_batch)
            data = self._getDCSResponse(query)
            temp_df = self._Response2DataFrame(data)
            DataFrames.append(temp_df)
            print(f" done in {round(time.time()-t0,2)}s")

        chamberHVDataFrame = pd.concat(DataFrames)

        # the next line does lumisection = (time - timeRunStart)/secs_per_lumi (modulo conversions)
        chamberHVDataFrame['RunLumisection'] = ((chamberHVDataFrame['Timestamp']/1000 - self.startTimestamp)//sec_per_ls).astype('int32') 
        self.HVDataFrame = chamberHVDataFrame



    def _generateQuery(self, chamberIDsList):
        """
        generates a query to be sent to the DCS Bridge for the parsed chambers.
        It only fetches vMon and v0Set, hardcoded.
        """
        targets_list = []    
        for chamberID in chamberIDsList:
            for electrode in self.ELECTRODE_NAMES:
                targets_list.append({"metric": "vMon", "tag": f"{chamberID}:HV:{electrode}"})
                targets_list.append({"metric": "v0Set", "tag": f"{chamberID}:HV:{electrode}"})

        query = {
            "from": self.startTimestamp,
            "to": self.stopTimestamp,
            "targets": targets_list
        }

        return query

    def _getDCSResponse(self, query):
        """
        Formats the query into an api request,
        parses the DCS bridge response 
        """
        req = request.Request(self.DCS_URL, headers={"Content-Type": "application/json"}, data=bytes(json.dumps(query), "utf-8"))
        resp = request.urlopen(req, timeout=300)
        responseCode = resp.getcode() # not used, 200 for success
        data = json.load(resp)

        return data

    def _Response2DataFrame(self, data):
        """
        converts the json like response (data) into a dataframe
        """
        res = []
        for measurement in data:
            monitorable = measurement["target"].split(",")[0]
            objectID = measurement["target"].split(",")[-1]
            target_Chamber = objectID.split(":")[0]
            target_Electrode = objectID.split(":")[-1]

            for DP in measurement["datapoints"]:
                res.append([target_Chamber, target_Electrode, monitorable, DP[0], DP[1]])

        df = pd.DataFrame(res,columns=["ChamberName","Electrode","Monitorable","Value","Timestamp"])
        return self._ReshapeDataframe(df)


    def _ReshapeDataframe(self, df):
        """
        simplifies the usage of the dataframe by having v0Set and vMon as 
        column headers rather than row entries. We start with a dataframe that, for a given electrode and timestamp, 
        has 2 entries: one for v0Set and one for vMon. Here I unify the 2 entries into a single one containing both values.
        """
        df['Datetime'] = pd.to_datetime(df['Timestamp']* 1000000,origin='unix') ## TIMESTAMPS are in ms
        df_vMon = df[(df["Monitorable"].str.contains("vMon"))].copy()
        df_v0Set = df[df["Monitorable"].str.contains("v0Set")].copy()
        extracted_v0Set = []
        # Iterate over each row in df1
        for _, row in df_vMon.iterrows():
            target_value_ch = row['ChamberName']
            target_value_el = row['Electrode']
            target_value_va = row['Value']
            target_value_ts = row['Timestamp']
            target_value_dt = row['Datetime']
            matching_df2 =  df_v0Set[(df_v0Set["ChamberName"] == target_value_ch) & (df_v0Set["Electrode"] == target_value_el) & (df_v0Set["Timestamp"] <= target_value_ts )].reset_index() ## df_v0Set of same ch / electrode predating the current target timestamp

            best_index = (matching_df2['Timestamp'] - target_value_ts).idxmin()
            latestv0Set = matching_df2['Value'].iloc[best_index]
            extracted_v0Set.append(latestv0Set)

        df_vMon["v0Set"] = extracted_v0Set
        df_vMon["vMon"] = df_vMon["Value"]
        return df_vMon
