import sys

import numpy as np
import pandas as pd
import praw # PRAW is a Python package for accessing the Reddit API
from praw import Reddit

# Importing a constant list of Reddit post fields to extract
from utils.constants import POST_FIELDS

# Function to connect to the Reddit API using PRAW
def connect_reddit(client_id, client_secret, user_agent) -> Reddit:
    try:
        # Create a Reddit instance using PRAW, with client credentials and user agent
        reddit = praw.Reddit(client_id=client_id,
                             client_secret=client_secret,
                             user_agent=user_agent)
        print("connected to reddit!") # Success message
        return reddit        # Return the connected Reddit instance
    except Exception as e:
        # If there's an error during connection, print the error and exit the program
        print(e)
        sys.exit(1)


# Function to extract posts from a specified subreddit
def extract_posts(reddit_instance: Reddit, subreddit: str, time_filter: str, limit=None):
    # Access the subreddit using the Reddit instance
    subreddit = reddit_instance.subreddit(subreddit)
    
    # Retrieve top posts based on the provided time_filter (e.g., 'day', 'week') and limit (e.g., number of posts)
    posts = subreddit.top(time_filter=time_filter, limit=limit)

    post_lists = []         # Initialize an empty list to store extracted posts

    # Iterate over each post retrieved
    for post in posts:
        post_dict = vars(post)        # Convert the post object into a dictionary
        # Extract only the fields listed in POST_FIELDS (defined in the constants) from each post
        post = {key: post_dict[key] for key in POST_FIELDS}
        post_lists.append(post)     # Add the extracted post to the list

    return post_lists    # Return the list of extracted posts


# Function to transform the extracted post data (cleaning and formatting)
def transform_data(post_df: pd.DataFrame):
    # Convert the 'created_utc' column (timestamp) to a readable datetime format
    post_df['created_utc'] = pd.to_datetime(post_df['created_utc'], unit='s')
    
    # Convert the 'over_18' column to boolean (True if post is NSFW, False otherwise)
    post_df['over_18'] = np.where((post_df['over_18'] == True), True, False)
    
    # Ensure 'author' column is treated as a string
    post_df['author'] = post_df['author'].astype(str)
    
    # Handle the 'edited' column: fill in missing values with the mode (most common value)
    edited_mode = post_df['edited'].mode()
    post_df['edited'] = np.where(post_df['edited'].isin([True, False]),
                                 post_df['edited'], edited_mode).astype(bool)
    
    # Convert the 'num_comments' and 'score' columns to integers
    post_df['num_comments'] = post_df['num_comments'].astype(int)
    post_df['score'] = post_df['score'].astype(int)
    
    # Ensure the 'title' column is treated as a string
    post_df['title'] = post_df['title'].astype(str)

    return post_df # Return the transformed DataFrame


# Function to save the transformed data to a CSV file
def load_data_to_csv(data: pd.DataFrame, path: str):
    # Write the DataFrame to a CSV file at the specified path without including row indices
    data.to_csv(path, index=False)