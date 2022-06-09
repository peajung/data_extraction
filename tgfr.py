import pandas as pd
import numpy as np
from datetime import datetime
import json
from urllib.request import urlopen

# Get denormalized-flights data
def get_denorm_data(from_param, to_param) :
    denorm_url = 'http://sfuelepcn1.thaiairways.co.th:3001/denormalized-flights?'
    skip = 'skip=0'
    limit = 'limit=0'
    parameter = denorm_url + from_param +'&' + to_param + '&' + skip + '&' + limit
    denorm = pd.read_json(parameter)
    denorm.drop_duplicates(subset=['flight_date', 'flight_number', 'departure_aerodrome_icao_code', 'aircraft_registration'],inplace = True)
    return denorm

# User input eOFP (Set Limit = 1000, default = 50)
def get_oefp_user_input(skip = '0', limit = '500') :
    skip = str(skip)
    limit = str(limit)

    url = "https://tgeofp.rtsp.us/api/v1/userinputs?skip="+skip+"&limit="+limit
    return pd.read_json(url)

# Get all OFP data (Set limit = 1000, default = 150)
def get_ofp() :
    url = "https://tgeofp.rtsp.us/api/v1/ofp?limit=1000"
    return pd.read_json(url)

# Get JSON of ofp data by specific flightplan id
def get_ofp_by_flightplan(flightplan) :
    try:
        url = "https://tgeofp.rtsp.us/api/v1/ofp/" + flightplan
        response = urlopen(url)
        data = json.loads(response.read())
        return data
    except Exception as e:
        return None

def create_fuelreport_df(denorm_df):
    fuel_report_df = denorm_df[denorm_df.fuelreport.notna()]['fuelreport'].apply(pd.Series)
    fuel_report_df.drop_duplicates(subset=['dep', 'flight_number', 'flight_date', 'aircraft_registration'], inplace=True)
    return fuel_report_df

def count_fuel_data(fuel_report_df):
    """Return DataFram contain number of Fuel report data each month"""
    
    fuel_summary_df = fuel_report_df.sort_values(['flight_date', 'flight_number']).reset_index()[['flight_date','flight_number','aircraft_registration','dep']]
    month = []
    year = []
    for date in fuel_summary_df.flight_date:
        date_time = pd.to_datetime(date)
        month.append(date_time.month)
        year.append(date_time.year)

    fuel_summary_df['month'] = month
    fuel_summary_df['year'] = year
    return fuel_summary_df.groupby(['year','month'])['flight_number'].count()

def merge_flightPlan_eofp(eofp,denorm) :
    """ger eofp dataFrame then use each flight plan to get information from flighplan database"""
    ## Initialize dataframe with eOFP
    df = eofp

    ## Get OFP data to create joint column on denorm
    for index, row in df.iterrows() :
        flightplan = df.iloc[index]["flightPlan"]
        
        # Get ofp flight information
        ofp_json = get_ofp_by_flightplan(flightplan)

        # Drop Unmatch FlightPlan ID
        if ofp_json == None : 
            df.drop(index = index, axis = 0)
            continue

        dep = ofp_json["flight_key"]['departure_aerodrome']['value']
        arr = ofp_json["flight_key"]['arrival_aerodrome']['value']
        flt_no = "THA" + ofp_json["flight_key"]["flight_number"]
        flt_date = datetime.strptime(ofp_json["flight_key"]["flight_date"],"%Y-%m-%dZ")
        flt_date = flt_date.strftime("%Y-%m-%dT%H:%M:%S.000Z")
        ac_reg = ofp_json["aircraft"]["aircraft_registration"]
        aircraft_registration = ac_reg[:2] + "-" + ac_reg[2:]
        imported_time = datetime.strptime(ofp_json["imported_time"], "%Y-%m-%dT%H:%M:%S.%fZ")
        #imported_time = imported_time.strftime("%Y-%m-%dT%H:%M:%S.000Z") ## TO String

        # Insert new column
        df.loc[index, "departure_aerodrome_icao_code"] = dep
        df.loc[index, "arrival_aerodrome_icao_code"] = arr
        df.loc[index, "flight_number"] = flt_no
        df.loc[index, "flight_date"] = flt_date
        df.loc[index, "aircraft_registration"] = aircraft_registration
        df.loc[index, "ofp_imported_time"] = imported_time
    
    ## Trim only eOFP data with new inserted column
    data_list = [
        "flightPlan","userInput","plannedCheckPoint","createdAt","updatedAt","ofp_imported_time",
        "departure_aerodrome_icao_code", "arrival_aerodrome_icao_code",
        "aircraft_registration", "flight_date", "flight_number",
        ]
    df = df[data_list]

    ## Drop NAN rows and reset index
    df = df.dropna(how = 'all')
    df = df.reset_index()

    # Unique keys
    joint_list = ["departure_aerodrome_icao_code", "arrival_aerodrome_icao_code", "aircraft_registration", "flight_date", "flight_number"]

    # Sort & Drop duplicate
    df = df.sort_values(by = ["ofp_imported_time"])
    df = df.drop_duplicates( subset = joint_list, keep = "last") # Keep lastest ofp imported_time

    ## Merge with denormalized
    df = pd.merge(df , denorm, how = "left", on = joint_list)
    df = df.sort_values(by = ["flight_date"], ascending=False)
    return df

def fuel_initiative_data(merged_df):
    """get eOFP data and return a DataFrame for fuel initiative analysis"""
    # Select some columns from merged_df
    eofp_data = merged_df[['flight_date', 'flight_number', "departure_aerodrome_icao_code", 'arrival_aerodrome_icao_code', 'aircraft_registration', 'planned_zfw']]
    eofp_data = eofp_data.join(merged_df.planned_fuel.apply(pd.Series)[['block_fuel','trip_fuel','taxi_fuel']])
    eofp_data = eofp_data.join(merged_df.fuelreport.apply(pd.Series).sample(20)[['std_date','plan_flt_time']])
    user_input_columns = merged_df.userInput.apply(pd.Series)[[
            'ramp_fuel',
            'est_zfw',
            'actual_zfw',
            'offblock_time',
            'offblock_fuel',
            'zfwcg',
            'pax_a',
            'pax_b',
            'pax_c',
            'pax_d',
            'pax_e',
            'infant',
            'airborne_time',
            'landing_time',
            'flight_time',
            'onblock_time',
            'onblock_fuel',
            'actual_burn_fuel',
            'block_time',
            'pf',
            'pm',
            'water_uplift',
            'water_remain',
        ]]
    eofp_data = eofp_data.join(user_input_columns)
    return eofp_data