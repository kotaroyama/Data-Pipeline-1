import pandas as pd

class MetricCalculator:
    def compute_session_metrics(self, df: pd.DataFrame):
        # Get converted sessions eg) sessions resulted in "purchase"
        converted_sessions = (
            df.sort_values("event_time")
            .groupby("user_session")["event_type"]
            .last()
        )
        converted_sessions = converted_sessions[converted_sessions == "purchase"].index
        df_converted = df[df["user_session"].isin(converted_sessions)]

        # Calculate time for conversion for each session resulted in conversion
        diff = (
            df_converted.groupby(by="user_session")["event_time"].max()
            - df_converted.groupby(by="user_session")["event_time"].min()
        )
        df_converted["time_to_conversion"] = df_converted["user_session"].map(diff)

        return df_converted

    def compute_product_metrics(self, df: pd.DataFrame):
        # Get all purchased products
        purchased_products = (
            df.sort_values("event_time")
            .groupby(["user_id", "product_id"])["event_type"]
            .last()
        )
        purchased_products = purchased_products[purchased_products == "purchase"].index
        df_purchased = df[
            df.set_index(["user_id", "product_id"]).index.isin(purchased_products)
        ]

        # Calculate average time for conversion for each product
        df_purchased["time_to_conversion"] = (
            df_purchased["event_time"] 
            - df_purchased.groupby(["user_id", "product_id"])["event_time"].transform("min")
        )
        df_purchased["time_to_conversion"] = df_purchased["time_to_conversion"].dt.total_seconds()
        avg_conversion_time = df_purchased.groupby(["product_id"])["time_to_conversion"].mean()
        total_purchases = df_purchased.groupby(["product_id"]).size()

        df_metrics = pd.concat(
            [avg_conversion_time, total_purchases], axis=1
        ).reset_index()
        df_metrics.columns = ["product_id", "avg_conversion_time", "total_purchases"]

        return df_metrics

    def compute_drop_rate(self, df: pd.DataFrame):
        # Get all sessions that didn't result in purchase
        dropped_sessions = (
            df.sort_values("event_time")
            .groupby("user_session")["event_type"]
            .last()
        )
        dropped_sessions = dropped_sessions[dropped_sessions != "purchase"].index
        df_dropped = df[df["user_session"].isin(dropped_sessions)]

        # Get converted sessions
        converted_sessions = (
            df.sort_values("event_time")
            .groupby("user_session")["event_type"]
            .last()
        )
        converted_sessions = converted_sessions[converted_sessions == "purchase"].index
        df_converted = df[df["user_session"].isin(converted_sessions)]

        # Calculate drop rate for every session
        num_of_dropped_sessions = len(df_dropped.groupby(by="user_session"))
        num_of_converted_sessions = len(df_converted.groupby(by="user_session"))
        total_sessions = num_of_dropped_sessions + num_of_converted_sessions
        drop_rate = num_of_dropped_sessions / total_sessions * 100
        return drop_rate