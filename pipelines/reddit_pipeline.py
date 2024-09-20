import pandas as pd

# Importing necessary functions and constants from other modules
from etls.reddit_etl import connect_reddit, extract_posts, transform_data, load_data_to_csv
from utils.constants import CLIENT_ID, SECRET, OUTPUT_PATH


"""
Executes the full Reddit ETL pipeline: connecting to Reddit, extracting posts, 
transforming the data, and loading it into a CSV file.
    
Args:
- file_name (str): The name of the CSV file where the data will be saved.
- subreddit (str): The subreddit from which to extract posts.
- time_filter (str): Time filter for posts (e.g., 'day', 'week', 'month'). Default is 'day'.
- limit (int): Maximum number of posts to extract. Default is None (no limit).
    
Returns:
- file_path (str): The full file path where the CSV is saved.
"""
    
def reddit_pipeline(file_name: str, subreddit: str, time_filter='day', limit=None):     
    # Connecting to reddit instance API using the provided credentials
    instance = connect_reddit(CLIENT_ID, SECRET, 'Airscholar Agent')
    # Extracting posts from the specified subreddit using the given time filter and limit
    posts = extract_posts(instance, subreddit, time_filter, limit)
    #Converting the extracted posts into a pandas DataFrame for easy manipulation
    post_df = pd.DataFrame(posts)
    # Transforming the extracted data (e.g., cleaning or formatting)
    post_df = transform_data(post_df)
    # Define the path to save the output CSV file and load the transformed data into it
    file_path = f'{OUTPUT_PATH}/{file_name}.csv'
    load_data_to_csv(post_df, file_path)

    # Return the file path of the saved CSV for future reference
    return file_path