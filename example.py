import datetime

import pandas as pd
from datetime import date

from numpy import int64

from fynesse import access, assess, address


def join():
    access.pandas_join_pp_pc()


def t():
    d = pd.to_datetime(date(2020, 1, 1))
    print(d.value / (60 * 60 * 24 * 1e9))
    print(pd.DataFrame({"time": [d]}).astype(int64) // 1e9)


def plot_date():
    data = pd.read_csv("./local_data/prices_coordinates_data.csv")
    assess.plot_date_view(data)


def plot_ans_graph(latitude, longitude, bounding_box, property_type):
    """Provide reference data and their summary near the predicting x.
    Provide a summary of data used in training and validation.
    Plot the map of London house prices"""
    data = pd.read_csv("./local_data/prices_coordinates_data.csv")
    ref = data.loc[
        (data.latitude > latitude - 0.005)
        & (data.latitude < latitude + 0.005)
        & (data.longitude < longitude + 0.005)
        & (data.longitude > longitude - 0.005)
        & (data.property_type == property_type)
    ]
    print("References near predict point: ")
    print(ref[["longitude", "latitude", "property_type", "date_of_transfer", "price"]])
    mean = ref["price"].mean()
    stdev = ref["price"].std()
    print(f"Ref mean: {mean}")
    print(f"Ref stdev: {stdev}")

    north, south, west, east = bounding_box
    ans = data.loc[
        (data.latitude > south)
        & (data.latitude < north)
        & (data.longitude < east)
        & (data.longitude > west)
        & (data.property_type == property_type)
    ]

    mean = ans["price"].mean()
    stdev = ans["price"].std()
    print(f"Bounding box (NSWE): {bounding_box}")
    print(f"Mean: {mean}")
    print(f"Stdev: {stdev}")

    assess.plot_loc_view(data)
    # assess.osm_view("London, UK", latitude=51.5, longitude=-0.2, box_height=0.1, box_width=0.1)


def predict():
    """Example prediction"""
    latitude = 51.4
    longitude = -0.3
    new_date = datetime.date(2024, 1, 1)
    property_type = "T"
    r2, y, bounding_box = address.predict_price(
        latitude, longitude, new_date, property_type
    )
    print(f"R2: {r2}")
    print(f"Prediction: {y}")
    plot_ans_graph(latitude, longitude, bounding_box, property_type)


if __name__ == "__main__":
    predict()
