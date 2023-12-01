from .config import *

from . import access

import osmnx as ox
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from sklearn.neighbors import BallTree


"""Place commands in this file to assess the data you have downloaded. How are missing values encoded, how are outliers encoded? What do columns represent, makes rure they are correctly labeled. How is the data indexed. Crete visualisation routines to assess the data (e.g. in bokeh). Ensure that date formats are correct and correctly timezoned."""


def get_bbox_around(latitude, longitude, bbox_length):
    """
    Returns the bounding box centred at (latitude, longitude) with side length bbox_length
    """
    north = latitude + bbox_length / 2
    south = latitude - bbox_length / 2
    east = longitude + bbox_length / 2
    west = longitude - bbox_length / 2
    return (north, south, east, west)


def get_pcd_nulls_df(db: access.Database):
    """
    Generates and returns the null counts for each column as a DataFrame
    """
    cols = db.get_columns("prices_coordinates_data")
    dfs = []
    for c in cols:
        dfs.append(
            db.execute_to_df(
                f"SELECT COUNT({c}) as {c} FROM prices_coordinates_data WHERE {c} = '';"
            )
        )
        print(f"Counted null values for column {c}.")
    null_dfs = pd.concat(dfs, axis=1)
    return null_dfs


def get_top_n_least_nulls(df: pd.DataFrame, n=10):
    """
    Returns a DataFrame showing the proportion of null values for each column, ranked by least to most.
    """
    null_proportions = df.isnull().sum() / len(df)
    null_proportions_df = pd.DataFrame(null_proportions, columns=["Null Proportion"])
    return null_proportions_df.sort_values("Null Proportion").head(n)


def get_pois_from_bbox(north, south, east, west, tags=None):
    """
    Returns POIs within the provided bounding box.
    """
    if tags == None:
        tags = {
            "amenity": True,
            "buildings": True,
            "historic": True,
            "leisure": True,
            "shop": True,
            "tourism": True,
        }

    return ox.features_from_bbox(north, south, east, west, tags)


def query(db: access.Database, latitude, longitude, bbox_length, start_date, end_date):
    """Request user input for some aspect of the data."""
    north = latitude + bbox_length / 2
    south = latitude - bbox_length / 2
    east = longitude + bbox_length / 2
    west = longitude - bbox_length / 2

    df = db.execute_to_df(
        f"""
        SELECT * FROM prices_coordinates_data
        WHERE
        date_of_transfer >= '{start_date}' AND
        date_of_transfer < '{end_date}' AND
        (latitude BETWEEN {south} AND {north}) AND
        (longitude BETWEEN {west} AND {east})
    """
    )

    gdf = convert_df_to_gdf(df)

    q_hi = gdf["price"].quantile(0.99)
    gdf = gdf[(gdf["price"] < q_hi)]
    return gdf


def filter_outliers_df(df):
    """
    Filters out outliers
    """
    q_hi = df["price"].quantile(0.99)
    return df[(df["price"] < q_hi)]


def convert_df_to_gdf(df):
    """
    Converts a DataFrame with longitude and latitude columns to a GeoDataFrame
    """
    geometry = gpd.points_from_xy(df["longitude"], df["latitude"])
    return gpd.GeoDataFrame(df, geometry=geometry, crs=4326)


def get_osm_features_df(gdf, pois, poi_key, poi_values):
    """
    Adds on osm features to gdf
    """
    df = gdf.copy()

    for place in poi_values:
        var = f"dist_to_nearest_{place}"
        joined_gdf = gpd.sjoin_nearest(
            gdf.to_crs(crs=3857),
            pois[pois[poi_key] == place].to_crs(crs=3857),
            how="left",
            distance_col=var,
        )
        df = df.join(joined_gdf[var])

    return df


def get_dist_nearest_corr_matrix(gdf, pois, poi_key, poi_values):
    """
    Returns the correlation matrix for distance nearest to POI
    """
    gdf = get_osm_features_df(gdf, pois, poi_key, poi_values)
    return gdf[["price"] + [f"dist_to_nearest_{t}" for t in poi_values]].corr()


def scatter_from_gdf_osm_features(df, columns):
    """
    Plots the relationship between price and the osm features
    """
    for col in columns:
        var = f"dist_to_nearest_{col}"
        data = pd.concat([df["price"], df[var]], axis=1)
        data.plot.scatter(x=var, y="price", ylim=(0, 10**7), xlim=(0, None))


def plot_corr_matrix(corr_matrix):
    """
    Plots given correlation matrix on heatmap.
    """
    f, ax = plt.subplots(figsize=(12, 9))
    sns.heatmap(corr_matrix, vmax=0.8, square=True)


def plot_corr_matrix_top_n(corr_matrix, n=10):
    """
    Finds most correlated n subset of given correlation matrix.
    """
    cols = corr_matrix.nlargest(n, "price")["price"].index
    sns.heatmap(
        corr_matrix.loc[cols, cols], annot=True, fmt=".3f", annot_kws={"size": 10}
    )
    plt.show()


def get_most_common_poi_values(pois, poi_key, n=20):
    """
    Returns the most common POI values for a given poi_key
    """
    return pois[poi_key].value_counts()[:n].keys().values


def categorical_feature_price_relation_boxplot(df, var, figsize=(8, 6)):
    """
    Plots a boxplot for a categorical feature against price
    """
    data = pd.concat([df["price"], df[var]], axis=1)
    f, ax = plt.subplots(figsize=figsize)
    fig = sns.boxplot(x=var, y="price", data=data)
    fig.axis(ymin=0)
    plt.xticks(rotation=90)


def categorical_feature_price_relation_violinplot(df, var, figsize=(8, 6)):
    """
    Plots a violinplot for a categorical feature against price
    """
    data = pd.concat([df["price"], df[var]], axis=1)
    f, ax = plt.subplots(figsize=figsize)
    fig = sns.violinplot(x=var, y="price", data=data)
    fig.axis(ymin=0)
    plt.xticks(rotation=90)


def visualise_categorial_features(df):
    """
    Visualises all categorical features by boxplots/violinplots.
    """
    vars = ["property_type", "new_build_flag", "tenure_type", "county"]
    for var in vars:
        if var == "county":
            figsize = (32, 6)
            categorical_feature_price_relation_boxplot(df, var, figsize)
        else:
            categorical_feature_price_relation_boxplot(df, var)
            categorical_feature_price_relation_violinplot(df, var)


def labelled(data_gdf, latitude, longitude, bbox_length):
    """Provide a labelled set of data ready for supervised learning."""
    bbox = get_bbox_around(latitude, longitude, bbox_length)
    pois = get_pois_from_bbox(*bbox)
    data_gdf = get_osm_features_df(
        data_gdf, pois, "amenity", ["school", "place_of_worship"]
    )
    data_gdf = get_osm_features_df(data_gdf, pois, "leisure", ["park"])
    data_gdf["local_median_price"] = calculate_local_median_price(data_gdf)
    return data_gdf


def calculate_local_median_price(gdf, k=10):
    """
    Calculates the median of the nearest k properties
    """
    k = min(k, gdf.shape[0])
    # Convert geometries to radians for BallTree
    gdf["geometry_radians"] = gdf["geometry"].apply(
        lambda geom: [np.radians(geom.y), np.radians(geom.x)]
    )

    # Create a BallTree for efficient spatial queries
    ball_tree = BallTree(np.vstack(gdf["geometry_radians"]), metric="haversine")

    # Query the BallTree for each point to find k nearest neighbors
    distances, indices = ball_tree.query(
        np.vstack(gdf["geometry_radians"]), k=k + 1
    )  # +1 because the point itself is included

    # Calculate median price for each set of neighbors
    local_median_prices = []
    for index_array in indices:
        prices = gdf.iloc[index_array]["price"]
        local_median_prices.append(prices.median())

    return local_median_prices
