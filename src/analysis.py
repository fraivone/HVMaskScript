import pandas as pd
import numpy as np
import scipy.interpolate as sp 
from tqdm import tqdm

def full_granularity_array(Start_Timestamp, Stop_Timestamp, granularity):
    """
    Input timestamps must be in seconds.
    Granularity is expected to be in milliseconds
    Generates a new time array with the requested granularity.
    This array will be used to have homogeneous data for each unit ID
    
    The DB returns data for each element (namely detUnit/electrode/metrics) queried.
    The DB only stores voltage data in case of a change. Therefore the data returned WON'T be
    equally spaced nor homogeneous among elements.
    However, all elements will have an entry for the first timestamp ("from" in the request).
    I guess the DB extrapolates this value when making responding to the request.
    Therefore all units are guaranteed with a first timestamp = Run_Start_Timestamp
    Here I just make sure they will all have time_max = Run_Stop_Timestamp
    """
    ## Run stop/start are expressed in s, while the DB times are in ms
    time_min = Start_Timestamp*1000
    time_max = Stop_Timestamp*1000
    #time_step = int((time_max-time_min)/granularity)
    
    return np.arange(time_min, time_max+granularity, granularity)


def clusterArray(time_array, granularity):
    """
    clusters the timestamps in time array that are closer than granularity*1.5 
    returns either an empty array if time_array is empty, or an array of sub-arrays 
    where each sub array is a cluster and contains the relative timestamps
    """
    if len(time_array)>1:
        ## array with Delta T of 2 subsequent trip timestamps 
        ## length is shortned by 1
        diff = time_array[1:]-time_array[:-1]
        ## cumulative counter of the cluster number.
        ## if 2 timestamps are separated by more than 150% granularity
        ## then it signals a new cluster which will be labelled with
        ## the next progressive number
        clusterLabels = np.cumsum(diff>=granularity*1.5)
        ## add 1 element to restore original size
        clusterLabels = np.concatenate([[0],clusterLabels])
        ## group clusters 
        clustersArray = [time_array[clusterLabels == label] for label in range(clusterLabels[-1]+1)]
        ## store only tstart and tend of each trip
        clustersArray = [ (min(t) , max(t)) for t in clustersArray]
        return clustersArray
    else:
        return np.array([])

def badLumisectionList(array_of_time_intervals, unitDataFrame):
    """
    array_of_time_intervals : contains the timestamps of start and end  as tuples (t_start, t_stop) of the events to be selected
    unitDataFrame: dataframe containing the data points for the unit considered
    returns: a list containing the lumisection corresponding to the time intervals selected
    """
    badLumisections = []
    for tStart,tEnd in array_of_time_intervals:
        sel = (unitDataFrame["Timestamp"]>= tStart) & (unitDataFrame["Timestamp"]<= tEnd)
        badLumisections += np.arange(unitDataFrame[ sel ]["RunLumisection"].min(), unitDataFrame[ sel ]["RunLumisection"].max()+1,1).tolist()

    return badLumisections
        
def processUnit(timestamp_array, unit_df):
    """
    a detector unit is made of 7 electrodes. Data points for
    each detector are recorded in case of a change. Therefore they are NOT
    aligned in time.
    This function interpolates the data points for each electrode on the 
    timestamp provided as input by <timestamp_array>. The datapoints are
    passed by means of the dataframe <unit_df>
    This function then returns, for each time stamp, the sum of v0Set and vMon over
    the 7 electrodes
    """
    electrodes = np.unique(unit_df["Electrode"].to_numpy())
    unit_id = np.unique(unit_df["ChamberName"].to_numpy())[0]
    validUnit = True
    
    vMonTotal_arr = np.zeros_like(timestamp_array)
    v0SetTotal_arr = np.zeros_like(timestamp_array)
    
    for e in electrodes:
        currElec_df = unit_df[unit_df["Electrode"]==e]
        N_Points = len(currElec_df)
        if N_Points == 0:
            print(f"{unit_id}_{e} has no data points. Skipped")
            validUnit = False
            break
        else:
            # electrode data points
            time_buf = currElec_df["Timestamp"].to_numpy()
            v0_buff = currElec_df["v0Set"].to_numpy()
            vMon_buf = currElec_df["vMon"].to_numpy()
            
            # build interpolation functions based on the data points
            ipo_vMon = sp.interp1d(time_buf, vMon_buf, kind='previous' ,fill_value=(vMon_buf[0], vMon_buf[-1]), bounds_error=False)
            ipo_v0 = sp.interp1d(time_buf, v0_buff, kind='previous' ,fill_value=(v0_buff[0], v0_buff[-1]), bounds_error=False)
            # use the interpolation function to generate a datapoint for each time bin 
            vMon_interpolated = ipo_vMon(timestamp_array)       
            v0_interpolated = ipo_v0(timestamp_array)
            # add voltage datapoints to the equivalent divider current array
            vMonTotal_arr = np.add(vMonTotal_arr, vMon_interpolated)
            v0SetTotal_arr = np.add(v0SetTotal_arr, v0_interpolated)
    
    return validUnit, vMonTotal_arr, v0SetTotal_arr


def analyze(DCS_df, equivalent_current_threshold, start_timestamp_s, stop_timestamp_s):
    """
    DCS_df: dataframe fetched from using the DCSchamberHVInfo class (HVDataFrame)
    equivalent_current_threshold: equivalent current threshold for abs(vSet - vMon) to label it as bad status
    start_timestamp_s: run start UTC timestamp in seconds
    stop_timestamp_s: run end UTC timestamp in seconds
    summaryDF contains, for each chamber, the avg and std for equivalentDividerCurrentMon and equivalentDividerCurrentSet
    """
    granularity = 20000 # in ms
    summary = []
    outputDict = {}
    time_new = full_granularity_array(start_timestamp_s, stop_timestamp_s, granularity)

    for id in tqdm(np.unique(DCS_df["ChamberName"].to_numpy()), desc='Analysing'):
        curr_df = DCS_df[DCS_df["ChamberName"]==id]
        validUnit, vMon, v0Set = processUnit(time_new, curr_df)
        outputDict[id] = []
        if validUnit:
            ieqMon = np.divide(vMon, 4.7)
            ieqSet = np.divide(v0Set, 4.7)
            summary.append([id, ieqMon.mean(), ieqMon.std(), ieqSet.mean(), ieqSet.std()])
        
            
            HVBadTimestamps = time_new[abs(ieqMon - ieqSet)>equivalent_current_threshold] ## array containing all bad HV timestamps
            HVBadClusters = clusterArray(HVBadTimestamps, granularity) ## array containing all bad HV clusters, expressed as time start/stop
            outputDict[id] += badLumisectionList(HVBadClusters, curr_df)
    
        else:
            ### all lumisection are assumed bad as there aren't enough 
            ### datapoints for at least 1 electrode
            outputDict[id] +=[-1]
        
    summaryDF = pd.DataFrame(summary, columns = ["ID", "MonEquivalentCurrent_mean", "MonEquivalentCurrent_std", "SetEquivalentCurrent_mean", "SetEquivalentCurrent_std"])        
    return summaryDF, outputDict
