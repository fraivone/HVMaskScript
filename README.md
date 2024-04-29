### HV Mask Generator

#### Steps
* Checks run start / stop times from the OMS bridge
* Fetches HV data (vMon and v0Set) for that specific run using the start/stop timestamps
* runs the analysis to spot *bad lumisection*:
    * The user sets a threshold for the *equivalent divider current* so that if abs(iEQSet- iEQMon) exceeds the threshold, the timestamp is marked as **BAD**
    * The datapoints are interpolated so that each chamber/electrode has the same number of points (ideally we'd want one point per Lumisection). I have (improperly) called the Delta T between 2 subsequent points *granularity*
    * The electrode's voltages are summed up and the equivalent divider currents are calculated. 
    * **BAD** timestamps closer than *granularity* are clustered and converted in Lumisections 

#### Outputs
* The pandas dataframe containing the DCS Bridge response is saved as `HVDCS_<run_number>.json`
* For each chamber, the bad lumisections are stored in the output file `BadHVLumi_<run_number>.json`, formatted as for the EfficiencyAnalyzer.py 
* A summary file `HVSummary_<run_number>.csv` containing, for each chamber, the AVG/STD Equiivalent Divider Current Mon/Set
#### Execution time
For an 11h run, it took roughly 8s to fetch the data, <1s to analyze it.


### Installation
* poetry shell
* poetry install
### Typical execution
Assumed the env variables `DCS_BRIDGE` and `OMS_BRIDGE` are properly set:
`python <run_number> <output_folder_path>