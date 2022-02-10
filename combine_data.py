import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import re
import time


def create_tweet_df(tweet_dataframe, price_dataframe, likes=None, keywords=None, extra_tweet_columns=None,
                    change_price_interval=None, percentage=False, sort_by_percentage=None,
                    sort_by_likes=False, ascending=False):
    """
    Take dataframe and column features from user. Return a dataframe for analyzing tweet-price relation.
    :param tweet_dataframe: Tweets that are scraped from twitter by twint module
    :param price_dataframe: 1minute price frames that are scraped with binance API
    :param likes: Minimum like amount for tweets
    :param keywords: Keywords to search in tweets. Ex: "btc|bitcoin|doge"

    :param extra_tweet_columns: List of column names for tweet dataframe. Possible options are: language,mentions,urls,
    photos,replies_count,retweets_count,likes_count,hashtags,cashtags,link,retweet,quote_url,video,thumbnail,near,geo,
    source,user_rt_id,user_rt,retweet_id,reply_to,retweet_date,translate,trans_src,trans_dest

    :param change_price_interval: List of time intervals (min or hr) to change price intervals. Ex:["3min", "5hr"]

    :param percentage: Percentage increase or decrease of time intervals.
    :param sort_by_percentage: Sort by a percentage type. Possible options are: "min", "hr", "day"

    :param sort_by_likes: Sort by tweet likes.

    :param ascending: If anything is being sorted, specify "ascending" or "descending".
    :return:
    """

    price_dataframe = price_dataframe[price_dataframe.columns[:5]]  # only take necessary columns
    price_dataframe.columns = ["timestamp", "open", "high", "low", "close"]  # rename columns

    column_list = ["date", "time", "tweet"]  # standard columns for tweets
    new_df = tweet_dataframe[column_list]

    pd.set_option('mode.chained_assignment', None)

    if extra_tweet_columns:  # add extra columns for more tweet data
        column_list.extend(extra_tweet_columns)

    if likes:  # if like amount is given
        if extra_tweet_columns:  # if likes aren't already added to df
            if "likes_count" not in extra_tweet_columns:
                column_list.extend(["likes_count"])  # add likes column
        else:
            column_list.extend(["likes_count"])
        new_df = tweet_dataframe[column_list]  # get df with likes

        new_df = new_df.loc[new_df["likes_count"] > likes]  # get tweets above the specified like amount
        if sort_by_likes:  # if df is sorted by likes
            new_df = new_df.sort_values("likes_count", ascending=ascending)  # sort by given order
            new_df.reset_index(drop=True, inplace=True)  # reset new df indexes

        if extra_tweet_columns:  # if likes aren't already added to dataframe by user, delete likes column.
            if "likes_count" not in extra_tweet_columns:
                new_df = new_df.drop('likes_count', 1)
        else:
            new_df = new_df.drop('likes_count', 1)

    if keywords:  # search for tweets that contain keywords
        new_df = new_df.loc[new_df["tweet"].str.contains(keywords, flags=re.I, regex=True)]

    new_df.reset_index(drop=True, inplace=True)  # reset row indexes so it doesn't get messed up

    minute, hour = 10, 1  # standard time intervals for Assignment 1
    if change_price_interval:  # change time intervals for minutes and hours if given any.
        for t in change_price_interval:
            if t[-3:] == "min":
                minute = int(t[:-3])
            if t[-2:] == "hr":
                hour = int(t[:-2])

    cols = ["Price at Day Start", "Price at tweet time", f"Price after {minute} min",
            f"Price after {hour} hour", "Price at day end"]  # standard price columns
    min_col, hour_col, day_col = "", "", ""
    if percentage:  # add "percentage change" columns if set to True by user.
        min_col, hour_col, day_col = f"{minute} minute %", f"{hour} hour %", f"24 hour %"
        cols.extend([min_col, hour_col, day_col])

    empty_df = pd.DataFrame(columns=cols)  # create an empty dataframe to append price data and concatenate with tweets.

    idx = 0  # index for calculating estimated time
    time_start = 0
    time_end = 0
    for index, row in new_df.iterrows():  # iterate through each tweet to find needed prices.
        #  code block for calculating estimated time.
        if idx == 0:
            time_start = time.time()
        if idx == 1:
            print(f"Estimated maximum time for creating dataframe: {(time_end-time_start)*new_df.shape[0]:.2f} seconds.")
            idx += 1

        # Tweet time is altered to start of the minute that tweet is tweeted.
        time1 = row["date"] + " " + row["time"][:-2] + "00"
        date_time = datetime.strptime(time1, "%Y-%m-%d %H:%M:%S")  # change time to datetime object.

        dt_day_start = date_time - timedelta(hours=date_time.hour,  # subtracting current time to get time at day start
                                             minutes=date_time.minute, seconds=date_time.second)
        date_time10 = date_time + timedelta(minutes=minute)  # add 10 minutes or else to get time of 10 min later
        date_time_1hr = date_time + timedelta(hours=hour)  # add 1hr or else to get time of 1hr later
        dt_day_end = dt_day_start + timedelta(hours=24)  # add 24hrs to day start time to get day end time

        # convert datetime objects into unix timestamps to match with prices.
        timestamp1 = int(datetime.timestamp(date_time))
        timestamp_ds = int(datetime.timestamp(dt_day_start))
        timestamp_10 = int(datetime.timestamp(date_time10))
        timestamp_1hr = int(datetime.timestamp(date_time_1hr))
        timestamp_de = int(datetime.timestamp(dt_day_end))

        def get_price(p_df, timestamp):
            """
            Function is defined to catch errors while getting price data. If price is not available returns NAN.
            :param p_df: Price dataframe
            :param timestamp: Unix timestamp
            :return: Price
            """
            try:
                return p_df.loc[p_df["timestamp"] / 1000 == timestamp]["open"].values[0]
            except IndexError:
                return np.nan

        # get necessary coin prices.
        price = get_price(price_dataframe, timestamp1)
        price_ds = get_price(price_dataframe, timestamp_ds)
        price_10 = get_price(price_dataframe, timestamp_10)
        price_1hr = get_price(price_dataframe, timestamp_1hr)
        price_de = get_price(price_dataframe, timestamp_de)

        price_row = [price_ds, price, price_10, price_1hr, price_de]  # join all prices in a row.

        if percentage:  # if percentage is wanted, calculate each interval of change and add as extra columns.
            if not pd.isna(price):  # check if price data is available before doing calculations.
                minute_diff = price_10-price
                minute_per = float(f"{(minute_diff / price)*100:.2f}")

                hour_diff = price_1hr - price
                hour_per = float(f"{(hour_diff / price)*100:.2f}")

                day_diff = price_de - price_ds
                day_per = float(f"{(day_diff / price)*100:.2f}")

                price_row.extend([minute_per, hour_per, day_per])
            else:  # if price data is not available add NAN for later clearing.
                price_row.extend([np.nan, np.nan, np.nan])

        empty_df = empty_df.append(pd.DataFrame([price_row], columns=empty_df.columns))  # add price row to empty df
        if idx == 0:
            time_end = time.time()
            idx += 1

    empty_df.reset_index(drop=True, inplace=True)

    tweet_with_price_df = pd.concat([new_df, empty_df], axis=1)  # concatenate tweet and price dataframes by axis=1

    if percentage:  # sort dataframe by specified interval of "percentage changes" if percentage exists.
        if sort_by_percentage:
            if sort_by_percentage == "min":
                tweet_with_price_df = tweet_with_price_df.sort_values(min_col, ascending=ascending)
                tweet_with_price_df.reset_index(drop=True, inplace=True)
            if sort_by_percentage == "hr":
                tweet_with_price_df = tweet_with_price_df.sort_values(hour_col, ascending=ascending)
                tweet_with_price_df.reset_index(drop=True, inplace=True)
            if sort_by_percentage == "day":
                tweet_with_price_df = tweet_with_price_df.sort_values(day_col, ascending=ascending)
                tweet_with_price_df.reset_index(drop=True, inplace=True)

    tweet_with_price_df = tweet_with_price_df.dropna(axis=0)  # drop all rows with NAN data
    tweet_with_price_df.reset_index(drop=True, inplace=True)

    return tweet_with_price_df  # return produced dataframe
