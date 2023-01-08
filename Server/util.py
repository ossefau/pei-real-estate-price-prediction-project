import json
import pickle
import numpy as np

__locations = None
__property_types = None
__attachement_types = None
__data_columns = None
__model = None 


def get_estimated_price(location,sqft,bed,bath,property_type,attachement):
    
    loc_index = __data_columns.index(location)
    p_type_index = __data_columns.index(property_type)
    attachement_index = __data_columns.index(attachement)


    x = np.zeros(len(__data_columns))
    x[0] = bed
    x[1] = bath
    x[2] = sqft
    if loc_index >= 0:
        x[loc_index] = 1
    if p_type_index >= 0:
        x[p_type_index] = 1
    if attachement_index >= 0:
        x[attachement_index] = 1

    return round(__model.predict([x])[0],0)


def get_location_names():
    return __locations

def get_property_types():
    return __property_types

def get_attachement_types():
    return __attachement_types

def load_saved_artifacts():
    print("loading saved artifacts...start")
    global __data_columns
    global __locations
    global __property_types
    global __attachement_types 

    with open("/Users/omarosefau/PRPP/Server/artifacts/columns.json",'r') as f:
        __data_columns = json.load(f)['data_columns']
        __locations = __data_columns[3:6]
        __property_types = __data_columns[6:10]
        __attachement_types = __data_columns[10:]
    
    global __model
    with open("/Users/omarosefau/PRPP/Model/pei_re_prediction_rf.pickle",'rb') as f:
        __model = pickle.load(f)
    

    print("loading saved artifacts...done")


if __name__ == '__main__':
    load_saved_artifacts()
    print(get_location_names())
    print(get_property_types())
    print(get_attachement_types())

