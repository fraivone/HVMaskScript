import pandas as pd
import calendar
from target_chambers import All_Units
import argparse
from OMSFetch import OMSrunInfo
from DCSFetch import DCSchamberHVInfo
from analysis import analyze
import json

def parse_arguments():
    parser = argparse.ArgumentParser(description="Parser for the run number (6-digit positive integer) and output folder path. Extracts, for each chamber, the lumisection values for which the vMon != v0Set")
    parser.add_argument("run_number", type=int, help="Run Number")
    parser.add_argument("output_folder", type=str, help="Folder where to store the csv output file")
    parser.add_argument('--thr', dest='ieq_threshold', default=5, type=int, help='equivalent current threshold for abs(iEQSet - iEQMon) to label the timestamp as bad. Default is 5uA')
    return parser.parse_args()
    
def dt2ts(dt):
    """
    Converts a datetime object to UTC timestamp
    naive datetime will be considered UTC.
    https://stackoverflow.com/questions/5067218/get-utc-timestamp-in-python-with-datetime
    """
    return calendar.timegm(dt.utctimetuple())



if __name__ == '__main__':
    args = parse_arguments()
    targetRun = args.run_number
    outfolder = args.output_folder
    threshold = args.ieq_threshold
    
    OMS = OMSrunInfo(targetRun)
    Run_Start_Timestamp =  int(OMS.getRunStartDatetime()) # seconds
    Run_Stop_Timestamp = int(OMS.getRunStopDatetime())    # seconds

    DCS = DCSchamberHVInfo(All_Units, Run_Start_Timestamp, Run_Stop_Timestamp)
    DCS.fetchData()
    df = DCS.HVDataFrame
    runHVSummary, badLSDict = analyze(df, threshold, Run_Start_Timestamp, Run_Stop_Timestamp)

    ## store outputs
    runHVSummary.to_csv(outfolder+f"/HVSummary_{targetRun}.csv")
    df.to_csv(outfolder+f"/HVDCS_{targetRun}.csv")
    
    json_output = outfolder+f"/BadHVLumi_{targetRun}.json"
    with open(json_output, "w") as jsonFile:
        json_data = json.dumps(badLSDict)
        jsonFile.write(json_data)

    print(f"Done")
    
