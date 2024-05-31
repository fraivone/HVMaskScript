from target_chambers import All_Units
import argparse
from OMSFetch import OMSrunInfo
from DCSFetch import DCSchamberHVInfo
from analysis import homogenizeDataPerLumisection
import json
from tqdm import tqdm
import numpy as np
import pandas as pd

def parse_arguments():
    parser = argparse.ArgumentParser(description="Parser for the run number (6-digit positive integer) and output folder path. Extracts, for each chamber, the lumisection values for which the vMon != v0Set")
    parser.add_argument("run_number", type=int, help="Run Number")
    parser.add_argument("output_folder", type=str, help="Folder where to store the csv output file")
    parser.add_argument('--thr', dest='ieq_threshold', default=5, type=int, help='equivalent current threshold for abs(iEQSet - iEQMon) to label the timestamp as bad. Default is 5uA')
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_arguments()
    targetRun = args.run_number
    outfolder = args.output_folder
    threshold = args.ieq_threshold
    
    OMS = OMSrunInfo(targetRun)
    Run_Start_Timestamp =  int(OMS.getRunStartDatetime()) # seconds
    Run_Stop_Timestamp = int(OMS.getRunStopDatetime())    # seconds
    DCS = DCSchamberHVInfo(Run_Start_Timestamp, Run_Stop_Timestamp)
    print(Run_Start_Timestamp,Run_Stop_Timestamp)

    badLSDict = {}
    summary = []
    for idx,ch in tqdm(enumerate(All_Units), miniters=1, desc="Marking bad HV Lumisections for the GEM HV units in target_chambers.py"):
        query = DCS._generateQuery([ch])
        data = DCS._getDCSResponse(query)
        ch_df = homogenizeDataPerLumisection(data, DCS.startTimestamp, DCS.stopTimestamp)

        ch_df["bad"] = abs(ch_df["iEqMon"] - ch_df["iEqSet"]) > threshold
        badLS = ch_df[ch_df["bad"] == True]["lumisection"].astype(int).to_numpy()
        badLS = np.sort(np.unique(badLS))
        badLSDict[ch] = badLS.tolist()
        summary.append([ch, ch_df["iEqMon"].mean(), ch_df["iEqMon"].std(), ch_df["iEqSet"].mean(), ch_df["iEqSet"].std()])

    ## store outputs
    runHVSummary = pd.DataFrame(summary, columns = ["ID", "MonEquivalentCurrent_mean", "MonEquivalentCurrent_std", "SetEquivalentCurrent_mean", "SetEquivalentCurrent_std"])
    runHVSummary.to_csv(outfolder+f"/HVSummary_{targetRun}.csv")
    
    json_output = outfolder+f"/BadHVLumi_{targetRun}.json"
    with open(json_output, "w") as jsonFile:
        json_data = json.dumps(badLSDict)
        jsonFile.write(json_data)
