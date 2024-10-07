import configparser
import os

# Initializing a ConfigParser object to read the configuration file
parser = configparser.ConfigParser()

# Read the configuration file located at ../config/config.conf
# The os.path.join and os.path.dirname(__file__) are used to create a relative path to the config file
#parser.read(os.path.join(os.path.dirname(__file__), '../config/config.conf'))

# Use an absolute path to ensure that the file is being read correctly
config_path = '/opt/airflow/config/config.conf'
parser.read(config_path)

# Check if the section [api_keys] exists
if not parser.has_section('api_keys'):
    raise Exception("The section [api_keys] is missing in the configuration file")


# Retrieve the Reddit API credentials from the 'api_keys' section of the configuration file
SECRET = parser.get('api_keys', 'reddit_secret_key')
CLIENT_ID = parser.get('api_keys', 'reddit_client_id')

# Retrieve database connection information from the 'database' section of the config file
DATABASE_HOST =  parser.get('database', 'database_host')
DATABASE_NAME =  parser.get('database', 'database_name')
DATABASE_PORT =  parser.get('database', 'database_port')
DATABASE_USER =  parser.get('database', 'database_username')
DATABASE_PASSWORD =  parser.get('database', 'database_password')

# AWS credentials and configurations from the 'aws' section of the config file
AWS_ACCESS_KEY_ID = parser.get('aws', 'aws_access_key_id')
AWS_ACCESS_KEY = parser.get('aws', 'aws_secret_access_key')
AWS_REGION = parser.get('aws', 'aws_region')
AWS_BUCKET_NAME = parser.get('aws', 'aws_bucket_name')

# File paths for input and output data are read from the 'file_paths' section of the config file
INPUT_PATH = parser.get('file_paths', 'input_path')
OUTPUT_PATH = parser.get('file_paths', 'output_path')

# A tuple containing the fields of interest for Reddit posts.
# These fields are used for extracting specific attributes of each post.
POST_FIELDS = (
    'id',             # Unique identifier for the post
    'title',          # Title of the Reddit post
    'score',          # Number of upvotes/downvotes
    'num_comments',   # Number of comments on the post
    'author',         # Username of the person who created the post
    'created_utc',    # UTC timestamp of when the post was created
    'url',            # URL link to the post
    'over_18',        # Whether the post is marked as NSFW (Not Safe for Work)
    'edited',         # Whether the post has been edited
    'spoiler',        # Whether the post contains spoilers
    'stickied'        # Whether the post is "stickied" to the top of the subreddit
)