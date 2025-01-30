from robocorp import workitems
from robocorp.tasks import task
from RPA.HTTP import HTTP
from RPA.JSON import JSON
from RPA.Tables import Tables

http = HTTP()
json = JSON()
table = Tables()

traffic_json_file_path = "output/traffic_data.json"
accident_rate_key = "NumericValue"
max_rate_required = 5.0
gender_key = "Dim1"
gender_required = "BTSX"
year_key = "TimeDim"
country_key = "SpatialDim"



@task
def produce_traffic_data():
    """Inhuman Insurance Inc. Artuficial Intelligence System automation. Produces traffic data work items"""
    http.download(

        url="https://github.com/robocorp/inhuman-insurance-inc/raw/main/RS_198.json", 
        target_file="output/traffic_data.json",
        overwrite=True

        )
    traffic_data = load_traffic_data_as_table()
    table.write_table_to_csv(traffic_data, "output/traffic_data.csv")
    filtered_data = filter_and_sort_traffic_data(traffic_data)
    latest_data = get_the_latest_data_by_year(filtered_data)
    payloads = transform_the_data(latest_data)
    save_work_items_payloads(payloads)
   



# producer functions

def load_traffic_data_as_table():
    json_data = json.load_json_from_file(traffic_json_file_path)
    return table.create_table(json_data["value"])

def filter_and_sort_traffic_data(data):

    table.filter_table_by_column(data, accident_rate_key, "<", max_rate_required)
    table.filter_table_by_column(data, gender_key, "==", gender_required)
    table.sort_table_by_column(data, year_key, False)
    return data

def get_the_latest_data_by_year(data):
    grouped_data = table.group_table_by_column(data, country_key)
    
    latest_data_by_country = []
    for group in grouped_data:
        first_row = table.pop_table_row(group)
        latest_data_by_country.append(first_row) 

    return latest_data_by_country

def transform_the_data(latest_data):
    payloads = []
    for row in latest_data:
        payload = dict(
            country = row[country_key],
            year = row[year_key],
            rate = row[accident_rate_key],

            )
        payloads.append(payload)
    return payloads

def save_work_items_payloads(payloads):
    for payload in payloads:
        variableS = dict(traffic_data=payload)
        workitems.outputs.create(variableS)
        















