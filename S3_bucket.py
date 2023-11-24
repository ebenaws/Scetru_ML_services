import boto3
from credentials import *

class S3FileCollector:
    """
    This System handles the process of downloading saved files from a given s3 location if
    file exist and returns relevant error messages where there are no files to process.
         1. list_buckets - This function collects all the available s3 buckets in the aws account.
         2. check_bucket_exists - This function checks if the given s3 buckets exist.
         3. check_bucket_contents - This function checks if the given s3 buckets has content after it has been confirmed that it exist.
         4. download_files - This function download all the files if has been confirmed that bucket has content.
         5. download_files_from_bucket - This functions runs the whole 
         
    
    """
    
    def __init__(self):
        self.s3_client = boto3.client('s3', aws_access_key_id = access_key, aws_secret_access_key = secret_access_key)
        
    def list_buckets(self):
        """
        List all S3 buckets in the AWS account.

        Returns:
        - A list of S3 bucket names.
        """
        try:
            # Get a list of S3 buckets
            response = self.s3_client.list_buckets()
            buckets = [bucket['Name'] for bucket in response['Buckets']]
            return buckets

        except Exception as e:
            return f"An error occurred: {str(e)}. Unable to list S3 buckets."
    
    def check_bucket_exists(self, bucket_name):
        """
        To check if the given bucket_name is part of the list return by list_buckets() function.
        Parameters:
        - bucket_name: Name of the S3 bucket
        
        Returns:
        - bucket_name if it is in the list.
        
        """
        # Check if the specified bucket exists in the list of all buckets
        all_buckets = self.list_buckets()
        return bucket_name in all_buckets

    def download_files(self, bucket_name, destination_folder='.'):
        """
        Download files from an S3 bucket to the specified destination folder.

        Parameters:
        - bucket_name: Name of the S3 bucket
        - destination_folder: Destination folder to save the downloaded files (default is the current working directory)

        Returns:
        - A list of downloaded file keys if files exist.
        - A message indicating that the bucket is empty if it has no files.
        - A message indicating that the bucket does not have 'Contents' if an error occurs.
        """
        try:
            response = self.s3_client.list_objects_v2(Bucket=bucket_name)
            contents = response.get('Contents', [])

            if contents:
                file_keys = [obj['Key'] for obj in contents]

                for key in file_keys:
                    download_path = f"{destination_folder}/{key}"
                    self.s3_client.download_file(bucket_name, key, download_path)

                return file_keys
            else:
                return f"The bucket '{bucket_name}' is empty. No files to download."

        except Exception as e:
            return f"An error occurred: {str(e)}. The bucket '{bucket_name}' may not have 'Contents.'"

    def check_bucket_contents(self, bucket_name):
        """
        Check if an S3 bucket has files to process.

        Parameters:
        - bucket_name: Name of the S3 bucket

        Returns:
        - A message indicating whether the bucket has files or is empty.
        - A message indicating that the bucket does not have 'Contents' if an error occurs.
        """
        try:
            response = self.s3_client.list_objects_v2(Bucket=bucket_name)
            contents = response.get('Contents', [])

            if contents:
                return f"The bucket '{bucket_name}' has files to process."
            else:
                return f"The bucket '{bucket_name}' is empty. No files to process."

        except Exception as e:
            return f"An error occurred: {str(e)}. The bucket '{bucket_name}' may not have 'Contents.'"
        
    def download_files_from_bucket(self, bucket_name, destination_folder='.'):
        """
        This a high-level function that orchestrates the process of checking if a specified S3 bucket exists, 
        checking if it has contents, and downloading files if there are files to process..

        Parameters:
        - bucket_name: Name of the S3 bucket
        - destination_folder: Destination folder to save the downloaded files (default is the current working directory)

        Returns:
        - A list of downloaded file keys if files exist.
        - A message indicating that the bucket is empty if it has no files.
        - A message indicating that the bucket does not have 'Contents' if an error occurs.
        """
        # Check if the specified bucket exists
        if not self.check_bucket_exists(bucket_name):
            print(f"The specified bucket '{bucket_name}' does not exist.")
            return

        # Check if the bucket has contents
        contents_check_result = self.check_bucket_contents(bucket_name)

        if "files to process" in contents_check_result:
            # Download files if the bucket has contents
            downloaded_files = self.download_files(bucket_name, destination_folder)
            print(f"Downloaded files from '{bucket_name}': {downloaded_files}")
        else:
            print(f"No files to download from '{bucket_name}'.")
        
        
