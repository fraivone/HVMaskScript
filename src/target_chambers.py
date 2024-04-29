"""
Assuming the naming conventions as in https://cmsgemonline-monitoring.app.cern.ch/d/f4Dw4oKVz/hv-monitoring?orgId=1
GE11-<endcap>-<chamber>L<layer> 
GE21-<endcap>-<chamber>L<layer><module>

where the module number goes as A,B,C,D for M1,M2,M3,M4

"""
# GE11
GE11unitIDs = [f"GE11-{'M' if re == -1 else 'P'}-{ch:02d}L{ly}" for re in [-1,1] for ly in [1,2] for ch in range(1,37)]

# GE21
GE21bareChamberIDs = ["GE21-P-16L1","GE21-M-16L1","GE21-M-18L1"]
GE21unitIDs = [f"{ch}{label}" for ch in GE21bareChamberIDs for label in ['A','B','C','D']]

# All
All_Units = GE11unitIDs + GE21unitIDs
