if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

def is_camel_case(s):
    return s != s.lower() and s != s.upper() and "_" not in s

@transformer
def transform(data, *args, **kwargs):

    # Convert lpep_pickup_datetime into lpep_pickup_date
    data["lpep_pickup_date"] = data["lpep_pickup_datetime"].dt.date
    #Rename VendorID in vendor_id
    data.rename(columns={"VendorID":"vendor_id"}, inplace=True) 
    # select data with passenger count is greater than 0
    data = data[(data["passenger_count"] > 0) & (data["trip_distance"] > 0)] 
    
    print("number of rows: ", data.shape[0])

    #unique value in vendor id column 
    print("unique vendor id: ", data.vendor_id.unique())

    print(len(data["lpep_pickup_date"].unique()))
    
    i = 0 
    for name_column in data.columns:
        if is_camel_case(name_column):
            i+=1

    print("number of column which needs to be renamed to snake case:", i)

    return data


@test
def test_rename_vendor_column(output, *args):
    assert "vendor_id" in output.columns, 'no column vendor_id'
@test
def check_zero_passenger_count(output, *args):
    assert output.passenger_count.isin([0]).sum()==0, "there is data with 0 passenger count"

@test
def check_zero_trip_distance(output, *args):
    assert output.trip_distance.isin([0]).sum() == 0, "there is data with 0 trip distance"