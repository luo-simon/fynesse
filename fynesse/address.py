# This file contains code for suporting addressing questions in the data

from . import assess

"""Address a particular question that arises from the data"""

import matplotlib.pyplot as plt
import statsmodels.api as sm
from datetime import timedelta
from sklearn.model_selection import train_test_split
import pandas as pd
from sklearn.metrics import mean_squared_error, r2_score
import math
import geopandas as 
from shapely.geometry import Point


def km_to_degrees(km):
    # Circumference of the Earth is ~40,000
    # 1 degree is around 40,000/360=111km
    return km / (40000 / 360)


def predict_price(db, latitude, longitude, date, property_type, bbox_length_km=15):
    """Price prediction for UK housing"""

    # Select bounding box around the housing location
    bbox_length = km_to_degrees(bbox_length_km)

    # Select data range around prediction date
    start_date = date - timedelta(300)
    end_date = date + timedelta(300)

    # Use data ecosystem to build a training set from relevant time period and location.
    data = assess.query(
        db,
        latitude,
        longitude,
        bbox_length=bbox_length,
        start_date=start_date,
        end_date=end_date,
    )
    df = assess.labelled(data, latitude, longitude, bbox_length)

    # Train a linear model
    X = df[
        [
            "local_median_price",
            "property_type",
            "dist_to_nearest_school",
            "dist_to_nearest_place_of_worship",
            "dist_to_nearest_park",
        ]
    ]
    X = pd.get_dummies(X, columns=["property_type"])
    y = df["price"]
    cols = [c for c in X.columns if c.startswith("property_type")]
    X[cols] = X[cols].astype(int)
    X = sm.add_constant(X)

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )
    model = sm.OLS(y_train, X_train)
    results = model.fit()

    # Validate quality of model
    y_pred = results.get_prediction(X_test).summary_frame(0.05)

    plt.scatter(y_test, y_pred["mean"])
    plt.xlabel("Actual Prices")
    plt.ylabel("Predicted Prices")
    plt.title("Actual Prices vs. Predicted Prices")
    plt.show()

    residuals = y_test - y_pred["mean"]
    plt.scatter(y_test, residuals)
    plt.axhline(y=0, color="red", linestyle="--")
    plt.xlabel("Actual Prices")
    plt.ylabel("Residuals")
    plt.title("Residual Plot")
    plt.show()

    # Calculate Mean Squared Error
    rmse = math.sqrt(mean_squared_error(y_test, y_pred["mean"]))

    # Calculate R-squared
    r2 = r2_score(y_test, y_pred["mean"])

    print(f"Root Mean Squared Error: {rmse}")
    print(f"R-squared: {r2}")

    # Warning if poor qality
    if r2 < 0.5:
        print(f"WARNING: Low R-squared, likely poor quality model.")

    # Provide prediction

    X_new_data = {
        "property_type": property_type,
        "latitude": [latitude],
        "longitude": [longitude],
        "geometry": Point(longitude, latitude),
    }
    data.loc[0] = X_new_data
    X_new_data_labelled = assess.labelled(data, latitude, longitude, bbox_length)
    # Train a linear model
    X_new = X_new_data_labelled[
        [
            "local_median_price",
            "property_type",
            "dist_to_nearest_school",
            "dist_to_nearest_place_of_worship",
            "dist_to_nearest_park",
        ]
    ]
    X_new = pd.get_dummies(X_new, columns=["property_type"])
    cols = [c for c in X_new.columns if c.startswith("property_type")]
    X_new[cols] = X_new[cols].astype(int)
    X_new = sm.add_constant(X_new)[0:1]

    prediction = results.get_prediction(X_new).summary_frame(0.05)["mean"]
    return prediction
