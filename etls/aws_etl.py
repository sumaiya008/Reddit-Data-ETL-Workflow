import s3fs  # s3fs allows interaction with AWS S3 using a filesystem-like interface
from utils.constants import AWS_ACCESS_KEY_ID, AWS_ACCESS_KEY # AWS credentials are stored in constants

# Function to establish a connection to S3 using AWS credentials
def connect_to_s3():
    try:
        # Creating an S3FileSystem object to interact with AWS S3
        # Anon=False means it will use authentication (non-public access)
        s3 = s3fs.S3FileSystem(anon=False,
                               key= AWS_ACCESS_KEY_ID,  # AWS Access Key ID from constants
                               secret=AWS_ACCESS_KEY)   # AWS Secret Access Key from constants
        return s3    # Return the connected S3 filesystem object
    except Exception as e:
        # If there's an error while connecting to S3, print the error
        print(e)

# Function to create an S3 bucket if it doesn't exist already
def create_bucket_if_not_exist(s3: s3fs.S3FileSystem, bucket:str):
    try:
        # Check if the specified bucket exists in S3
        if not s3.exists(bucket):
            s3.mkdir(bucket) # If the bucket does not exist, create it
            print("Bucket created")
        else :
            print("Bucket already exists")
    except Exception as e:
        print(e)

# Function to upload a file to the S3 bucket
def upload_to_s3(s3: s3fs.S3FileSystem, file_path: str, bucket:str, s3_file_name: str):
    try:
        # Upload the file from the local system to the S3 bucket under the 'raw' folder
        s3.put(file_path, bucket+'/raw/'+ s3_file_name)
        print('File uploaded to s3')
    except FileNotFoundError:
        # If the specified file is not found on the local system, print an error message
        print('The file was not found')