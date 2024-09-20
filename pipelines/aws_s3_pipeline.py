from etls.aws_etl import connect_to_s3, create_bucket_if_not_exist, upload_to_s3
from utils.constants import AWS_BUCKET_NAME


# This function defines the pipeline to upload data to AWS S3
def upload_s3_pipeline(ti):
    """
    This function orchestrates the process of uploading a file to S3 by:
    1. Retrieving the file path (produced by a previous task) using XCom.
    2. Connecting to S3.
    3. Ensuring the S3 bucket exists, creating it if it doesn't.
    4. Uploading the file to the S3 bucket.
    
    Args:
    - ti: Task instance, used for pulling data (file path) from a previous task via XCom.
    """

    # Step 1: Pull the file path from the 'reddit_extraction' task using XCom.
    # ti.xcom_pull() retrieves the return value (file path) from a previously executed task.
    file_path = ti.xcom_pull(task_ids='reddit_extraction', key='return_value')

    # Step 2: Connect to the AWS S3 service using stored credentials.
    s3 = connect_to_s3()
    
    # Step 3: Ensure that the S3 bucket exists. If it doesn't, create the bucket.
    create_bucket_if_not_exist(s3, AWS_BUCKET_NAME)
    
    # Step 4: Upload the file to the S3 bucket. 
    # The file name is extracted from the file path by splitting the path and getting the last part (file name).
    upload_to_s3(s3, file_path, AWS_BUCKET_NAME, file_path.split('/')[-1])