import numpy as np
import pandas as pd

# define function to check take off flaps and decel approach
def takeoff_flap(aircraft, flaps):
    if aircraft == 'A359':
        if flaps > 16.5:
            return False
        else:
            return True
    else:
        if flaps > 5:
            return False
        else:
            return True

def decel_app(alt):
    if alt > 2000:
        return False
    else:
        return True

def b787_type(call_sign, aircraft_type):
    reduced_callsign = call_sign[3:5]
    if reduced_callsign == 'TQ':
        final_type = 'B788'
    elif reduced_callsign == 'TW':
        final_type = 'B789'
    else:
        final_type = aircraft_type
    return final_type    