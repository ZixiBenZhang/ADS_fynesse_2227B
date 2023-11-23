# This file contains code for suporting addressing questions in the data

"""# Here are some of the imports we might expect 
import sklearn.model_selection  as ms
import sklearn.linear_model as lm
import sklearn.svm as svm
import sklearn.naive_bayes as naive_bayes
import sklearn.tree as tree

import GPy
import torch
import tensorflow as tf

# Or if it's a statistical analysis
import scipy.stats"""

import datetime

import numpy as np
import pandas as pd
import sklearn

from . import assess

"""Address a particular question that arises from the data"""


def predict_price(
    dataset, latitude, longitude, date, property_type
) -> tuple[float, list, tuple]:
    """Price prediction for UK housing."""

    # Construct learning set: near (latitude, longitude), all dates, of same property_type
    dataset, bounding_box = get_learning_dataset(
        dataset, latitude, longitude, date, property_type
    )
    dataset.loc[:, "date_of_transfer"] = dataset["date_of_transfer"].apply(
        lambda x: round(x.value / (24 * 60 * 60 * 1e9))
    )

    # Split the dataset
    idx = np.arange(len(dataset))
    # np.random.shuffle(idx)
    split_idx = round(0.8 * len(dataset))
    train_set, val_set = (dataset.iloc[idx[:split_idx]], dataset.iloc[idx[split_idx:]])

    # Linear regression model
    model = sklearn.linear_model.LinearRegression()

    # We assume price is near-linear to longitude & latitude within a small bounding box
    x_design = train_set.loc[
        :,
        [
            "latitude",
            "longitude",
            "date_of_transfer",
        ],
    ]
    reg = model.fit(x_design, train_set.loc[:, "price"])

    x_val_design = val_set.loc[
        :,
        [
            "latitude",
            "longitude",
            "date_of_transfer",
        ],
    ]
    score_R2 = reg.score(x_val_design, val_set.loc[:, "price"])
    # print(f"R2: {score_R2}")

    x_pred_design = pd.DataFrame(
        [
            [
                latitude,
                longitude,
                round(pd.to_datetime(date).value / (24 * 60 * 60 * 1e9)),
            ]
        ],
        columns=["latitude", "longitude", "date_of_transfer"],
    )
    y_pred = reg.predict(x_pred_design)

    return score_R2, y_pred, bounding_box


def get_learning_dataset(
    dataset, latitude, longitude, date, property_type
) -> tuple[pd.DataFrame, tuple]:
    # Todo: load data from assess.data(). Load data using SQL within a large box??
    # data = pd.read_csv("./local_data/prices_coordinates_data.csv")
    # data.loc[:, "date_of_transfer"] = pd.to_datetime(data["date_of_transfer"])
    data = assess.labelled(assess.data(dataset))

    date_range = _get_date_range(date)
    bounding_box = _get_bounding_box(
        latitude, longitude, data, date_range, property_type
    )

    # Get features about surrounding buildings (over history?) from OpenStreetMap
    # OSM features don't contribute a lot of information that are stable and not reflected in house prices, so ignored
    osm_features = _get_OSM_features(
        bounding_box=bounding_box, date_range=date_range, property_type=property_type
    )
    osm_features = pd.DataFrame(osm_features, columns=None)

    # Since mean and variance of each property_type are different, we build different models for them
    data_subset = _get_pcd_data(data, bounding_box, date_range, property_type)

    # Currently only takes data_subset, osm_features ignored
    train_set = _join_pcd_osm(data_subset, osm_features)
    return train_set, bounding_box


def _get_OSM_features(bounding_box, date_range, property_type) -> tuple:
    north, south, west, east = bounding_box
    date_lb, date_ub = date_range
    return (0,)


def _join_pcd_osm(pcd_data: pd.DataFrame, osm_features: pd.DataFrame) -> pd.DataFrame:
    return pcd_data


def _get_pcd_data(
    data: pd.DataFrame, bounding_box, date_range, property_type
) -> pd.DataFrame:
    north, south, west, east = bounding_box
    date_lb, date_ub = date_range

    data_ = data.loc[
        (north > data.latitude)
        & (data.latitude > south)
        & (east > data.longitude)
        & (data.longitude > west)
        & (pd.Timestamp(date_ub) > data.date_of_transfer)
        & (data.date_of_transfer > pd.Timestamp(date_lb))
        & (data.property_type == property_type)
    ]
    return data_


def _get_bounding_box(
    latitude, longitude, data: pd.DataFrame, date_range, property_type
) -> tuple:
    threshold = 10000

    box_height = 0.01
    box_width = 0.01
    north = latitude + box_height / 2
    south = latitude - box_height / 2
    west = longitude - box_width / 2
    east = longitude + box_width / 2
    data_ = _get_pcd_data(data, (north, south, west, east), date_range, property_type)
    while len(data_.index) < threshold:
        box_height *= 2
        box_width *= 2
        north = latitude + box_height / 2
        south = latitude - box_height / 2
        west = longitude - box_width / 2
        east = longitude + box_width / 2
        data_ = _get_pcd_data(
            data, (north, south, west, east), date_range, property_type
        )
    return north, south, west, east


def _get_date_range(date: datetime.date) -> tuple[datetime.date, datetime.date]:
    return datetime.date(date.year - 5, 1, 1), datetime.date(date.year + 5, 12, 31)
