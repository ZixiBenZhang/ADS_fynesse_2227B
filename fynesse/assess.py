from datetime import date
from pprint import pprint

import matplotlib
import mlai.plot as plot
import osmnx as ox
import pandas as pd
from matplotlib import pyplot as plt

from . import access

"""These are the types of import we might expect in this file
import pandas
import bokeh
import seaborn
import matplotlib.pyplot as plt
import sklearn.decomposition as decomposition
import sklearn.feature_extraction"""

"""Place commands in this file to assess the data you have downloaded. How are missing values encoded, 
how are outliers encoded? What do columns represent, makes rure they are correctly labeled. How is the data indexed. 
Create visualisation routines to assess the data (e.g. in bokeh). Ensure that date formats are correct and correctly 
time-zoned. """


def data(df: pd.DataFrame) -> pd.DataFrame:
    """Load the data from access and ensure missing values are correctly encoded as well as indices correct,
    column names informative, date and times correctly formatted. Return a structured data structure such as a data
    frame."""
    df.loc[:, "date_of_transfer"] = pd.to_datetime(df.loc[:, "date_of_transfer"])
    return df


def query(conn, sql_command: str) -> tuple:
    """Request user input for some aspect of the data."""
    # conn = access.create_connection(username, password, url, "property_prices", port)
    cur = conn.cursor()
    cur.execute(sql_command)
    rows = cur.fetchall()
    return rows


def plot_date_view(dataset: pd.DataFrame):
    """Plot a diagram of house prices in London across date_of_transfer"""
    data_: pd.DataFrame = dataset.loc[
        (52 > dataset.latitude)
        & (dataset.latitude > 51)
        & (0.5 > dataset.longitude)
        & (dataset.longitude > -0.5)
    ]
    data_.loc[:, "date_of_transfer"] = pd.to_datetime(data_.loc[:, "date_of_transfer"])

    flat = data_[data_.property_type == "F"]
    semidetached = data_[data_.property_type == "S"]
    detached = data_[data_.property_type == "D"]
    terraced = data_[data_.property_type == "T"]
    other = data_[data_.property_type == "O"]

    fig, axes = plt.subplots(1, 5, sharey=True, figsize=(10, 5))

    # ax.set_xlim([west, east])
    # ax.set_ylim([south, north])
    for ax in axes:
        ax.set_xlabel("date")
        ax.set_ylabel("price")
        ax.set_yscale("log")

    axes[0].scatter(
        flat["date_of_transfer"], flat["price"], alpha=0.1, color="red", s=0.05
    )
    axes[0].set_title("Flat")
    axes[1].scatter(
        detached["date_of_transfer"],
        detached["price"],
        alpha=0.1,
        color="green",
        s=0.05,
    )
    axes[1].set_title("Detached")
    axes[2].scatter(
        semidetached["date_of_transfer"],
        semidetached["price"],
        alpha=0.3,
        color="blue",
        s=0.05,
    )
    axes[2].set_title("Semidetached")
    axes[3].scatter(
        terraced["date_of_transfer"],
        terraced["price"],
        alpha=0.05,
        color="purple",
        s=0.05,
    )
    axes[3].set_title("Terraced")
    axes[4].scatter(
        other["date_of_transfer"], other["price"], alpha=0.1, color="grey", s=0.05
    )
    axes[4].set_title("Other")

    for ax in axes:
        ax.tick_params(labelrotation=45)
    plt.tight_layout()
    plt.show()


def plot_loc_view(dataset: pd.DataFrame):
    """Plot a map of house prices in London w.r.t. latitude & longitude"""
    north = 52
    south = 51
    west = -0.7
    east = 0.3

    data_: pd.DataFrame = dataset.loc[
        (north > dataset.latitude)
        & (dataset.latitude > south)
        & (east > dataset.longitude)
        & (dataset.longitude > west)
    ]
    data_.loc[:, "date_of_transfer"] = pd.to_datetime(data_.loc[:, "date_of_transfer"])
    data_ = data_.loc[
        (pd.Timestamp(date(2022, 12, 31)) > data_.date_of_transfer)
        & (data_.date_of_transfer > pd.Timestamp(date(2022, 1, 1)))
    ]

    flat = data_[data_.property_type == "F"]
    semidetached = data_[data_.property_type == "S"]
    detached = data_[data_.property_type == "D"]
    terraced = data_[data_.property_type == "T"]
    other = data_[data_.property_type == "O"]

    fig, ax = plt.subplots(figsize=(8, 6))

    ax.set_xlim([west, east])
    ax.set_ylim([south, north])
    ax.set_xlabel("longitude")
    ax.set_ylabel("latitude")

    ax.scatter(
        flat["longitude"],
        flat["latitude"],
        alpha=1,
        c=flat["price"],
        s=0.01,
        cmap="Reds",
        norm=matplotlib.colors.LogNorm(),
    )
    ax.scatter(
        detached["longitude"],
        detached["latitude"],
        alpha=1,
        c=detached["price"],
        s=0.01,
        cmap="Greens",
        norm=matplotlib.colors.LogNorm(),
    )
    ax.scatter(
        semidetached["longitude"],
        semidetached["latitude"],
        alpha=1,
        c=semidetached["price"],
        s=0.01,
        cmap="Blues",
        norm=matplotlib.colors.LogNorm(),
    )
    ax.scatter(
        terraced["longitude"],
        terraced["latitude"],
        alpha=1,
        c=terraced["price"],
        s=0.01,
        cmap="Purples",
        norm=matplotlib.colors.LogNorm(),
    )
    ax.scatter(
        other["longitude"],
        other["latitude"],
        alpha=1,
        c=other["price"],
        s=0.01,
        cmap="Greys",
        norm=matplotlib.colors.LogNorm(),
    )
    fig.colorbar(plt.cm.ScalarMappable(cmap="Greys"), ax=ax)

    plt.tight_layout()
    plt.show()


def osm_view(
    place_name: str,
    latitude: float,
    longitude: float,
    box_height: float,
    box_width: float,
    tags=None,
):
    """Provide a view of the data that allows the user to verify some aspect of its quality."""

    if tags is None:
        tags = {
            "amenity": True,
            "building": True,
            "historic": True,
            "leisure": True,
            "shop": True,
            "tourism": True,
        }

    north = latitude + box_height / 2
    south = latitude - box_height / 2
    west = longitude - box_width / 2
    east = longitude + box_width / 2

    pois = ox.features_from_bbox(north, south, east, west, tags)
    pprint(pois.loc[:, ["building", "historic", "leisure", "tourism"]].iloc[80:100, :])

    graph = ox.graph_from_bbox(north, south, east, west)

    # Retrieve nodes and edges
    nodes, edges = ox.graph_to_gdfs(graph)

    # Get place boundary related to the place name as a geodataframe
    area = ox.geocode_to_gdf(place_name)

    fig, ax = plt.subplots(figsize=plot.big_figsize)

    # Plot the footprint
    area.plot(ax=ax, facecolor="white")

    # Plot street edges
    edges.plot(ax=ax, linewidth=1, edgecolor="dimgray")

    ax.set_xlim([west, east])
    ax.set_ylim([south, north])
    ax.set_xlabel("longitude")
    ax.set_ylabel("latitude")

    # Plot all POIs
    pois.plot(ax=ax, color="blue", alpha=0.7, markersize=10)
    plt.tight_layout()
    plt.show()


def labelled(dataset) -> pd.DataFrame:
    """Provide a labelled set of data ready for supervised learning."""
    dataset = dataset[
        ["price", "latitude", "longitude", "date_of_transfer", "property_type"]
    ]
    return dataset
