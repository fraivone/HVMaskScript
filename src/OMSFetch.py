import os
import json
from urllib import request
import datetime

class OMSrunInfo:
    """
    Class to ease the querying of the OMS bridge for run info data.
    Timestamps always in UTC format.

    debug example from command prompt:
    
    wget --output-document=output.txt --post-data='{"run-number": 379765}'   --header='Content-Type:application/json $OMS_BRIDGE
    
    server response (stored in output.txt)
    [ 
    {'target':'vMon,GE11-P-16L1:HV:DRIFT', 'datapoints':[[VALUE,TIMESTAMP],...,[VALUE,TIMESTAMP]]},
    {},
    ...,
    ]

    server response (stored in output.txt)
    {"data":
        {"b_field":3.8,
         ...
         "l1_hlt_collisions2024/v79"
        },
     "result":"ok"
    }

    """

    def __init__(self,run:int):
        self.OMS_URL = os.environ['OMS_BRIDGE']

        self.run = run
        self.data : dict
        self.getOMSResponse()
        
    def getOMSResponse(self):
        """
        fetches the OMS Bridge to get info about a given run number.
        The full response can be cout wiht the function printOMSResponse
        """
        data = {"run-number": self.run}
        req = request.Request(self.OMS_URL, headers={"Content-Type": "application/json"}, data=bytes(json.dumps(data), "utf-8"))
        resp = request.urlopen(req)
        self.data = json.load(resp)

    def printInfo(self):
        for key in self.data.keys():
            print(f"{key:<30}{self.data[key]}")

    def getRunStartDatetime(self):
        """ 
        parses the OMS response to get the start time field 
        and converts it into a datetetime object
        """
        return datetime.datetime.strptime(self.data['data']['start_time'],'%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=datetime.timezone.utc).timestamp()
    def getRunStopDatetime(self):
        """ 
        parses the OMS response to get the end time field 
        and converts it into a datetetime object
        """
        return datetime.datetime.strptime(self.data['data']['end_time'],'%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=datetime.timezone.utc).timestamp()
    def getRunSeconds_per_Lumisection(self):
        """
        used only for debugging
        extracts the duration of 1 LumiSection in seconds
        this factor helps converting HV events (i.e. trips)  
        into Lumisection values so that a trip can be masked from the analysis
        """
        return float(self.data['data']['duration'])/self.data['data']['last_lumisection_number']
